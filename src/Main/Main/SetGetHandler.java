package Main; // Defines the package name where this class belongs

import java.io.*; // Import classes for input and output operations
import java.net.Socket; // Import Socket class to handle network connections
import java.util.concurrent.ConcurrentHashMap; // Import thread-safe hash map for storing data safely across threads
import Main.ReplicaClient;

// This class handles the SET and GET commands from clients, runs in its own thread
public class SetGetHandler extends Thread {
    private final Socket client; // The connection socket for communicating with the client
    private final String[] args; // The command and arguments sent by the client, split into parts
    private final ConcurrentHashMap<String, String> store; // A thread-safe map storing key-value pairs
    private final ConcurrentHashMap<String, Long> expiry; // A map storing expiration times (in milliseconds) for keys
    private final boolean replicaStatus;// keeping track of replicastatus as we have to propagate set command to replica

    // Constructor to create a handler for a client connection with provided command
    // arguments and data maps
    public SetGetHandler(Socket client, String[] args, ConcurrentHashMap<String, String> store,
            ConcurrentHashMap<String, Long> expiry, boolean replicaStatus) {
        this.client = client; // Store the client's socket connection
        this.args = args; // Store the command arguments
        this.store = store; // Store the reference to the shared key-value store
        this.expiry = expiry; // Store the reference to the expiry map
        this.replicaStatus = replicaStatus;
    }

    // This method runs automatically when the thread starts
    @Override
    public void run() {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()))) {
            // Create a writer to send responses back to the client through the socket

            String command = args[0].toUpperCase(); // Extract the first word from command as uppercase (e.g. SET, GET)
            System.out.println("replicastaus:- " + replicaStatus);
            switch (command) { // Decide what to do based on the command
                case "SET":
                    // Check if at least key and value are provided after SET command
                    if (args.length < 3) {
                        writer.write("-ERR wrong number of arguments for 'SET'\r\n"); // Tell client error for too few
                        // arguments
                        writer.flush(); // Make sure message is sent immediately
                        client.close(); // Close client connection because command is invalid
                        return; // Stop processing further to avoid errors
                    }

                    String key = args[1]; // The second argument is the key to set
                    String value = args[2]; // The third argument is the value to set for the key

                    Long expireAt = null; // Variable to store expiration time of the key, if set (null means no
                    // expiration)
                    boolean nx = false; // Flag to mark if 'NX' option is used (only set if key does not exist)
                    boolean xx = false; // Flag to mark if 'XX' option is used (only set if key already exists)

                    // Loop through remaining arguments to find optional parameters (EX, PX, NX, XX)
                    for (int i = 3; i < args.length; i++) {
                        String option = args[i].toUpperCase(); // Get option in uppercase for comparison

                        switch (option) { // Handle different options
                            case "EX": // Expire time in seconds
                                if (i + 1 < args.length) { // Check if next argument exists (time value)
                                    int seconds = Integer.parseInt(args[++i]); // Parse next argument as integer seconds
                                    expireAt = System.currentTimeMillis() + seconds * 1000L; /*
                                                                                              * Calculating expiry time
                                                                                              * i.e current time + given
                                                                                              * expiry limit
                                                                                              */

                                    if (replicaStatus) {
                                        ReplicaClient.ReplicaSetCommand("*4\r\n" + "$3\r\n" + "SET\r\n" + "$"
                                                + args[1].length() + "\r\n" + args[1] + "\r\n$" + args[2].length()
                                                + "\r\n"
                                                + args[2] + "\r\n$" + expireAt.toString().length() + "\r\n" + expireAt);
                                    }
                                } else {
                                    writer.write("-ERR syntax error in EX\r\n"); // Send error if missing time value
                                    writer.flush();
                                    client.close();
                                    return;
                                }
                                break;

                            case "PX": // Expire time in milliseconds
                                if (i + 1 < args.length) { // Check if next argument exists (time value)
                                    int ms = Integer.parseInt(args[++i]); // Parse next argument as integer milliseconds
                                    expireAt = System.currentTimeMillis() + ms; // Calculate expiry time in future
                                    if (replicaStatus) {
                                        ReplicaClient.ReplicaSetCommand("*4\r\n" + "$3\r\n" + "SET\r\n" + "$"
                                                + args[1].length() + "\r\n" + args[1] + "\r\n$" + args[2].length()
                                                + "\r\n"
                                                + args[2] + "\r\n$" + expireAt.toString().length() + "\r\n" + expireAt);
                                    }
                                } else {
                                    writer.write("-ERR syntax error in PX\r\n"); // Send error if missing time value
                                    writer.flush();
                                    client.close();
                                    return;
                                }
                                break;

                            case "NX": // Only set if key does not exist already
                                nx = true; // Mark the NX flag true
                                break;

                            case "XX": // Only set if key already exists
                                xx = true; // Mark the XX flag true
                                break;

                            default: // If option is unknown
                                writer.write("-ERR unknown option for SET\r\n"); // Inform client about invalid option
                                writer.flush();
                                client.close();
                                return; // Stop processing due to error
                        }
                    }

                    // Check if both NX and XX are set, which is invalid usage
                    if (nx && xx) {
                        writer.write("-ERR NX and XX options at the same time are not allowed\r\n"); // Error message
                        writer.flush();
                        client.close();
                        return; // Stop processing because options conflict
                    }

                    boolean keyExists = store.containsKey(key); // Check if key currently exists in the store

                    // Conditions to decide if we actually set the value or not
                    if ((nx && keyExists) || (xx && !keyExists)) {
                        // NX is true and key exists => do NOT set
                        // OR XX is true and key does NOT exist => do NOT set
                        writer.write("$-1\r\n"); // Respond with null bulk string indicating no operation done
                    } else {
                        store.put(key, value); // Put the key-value pair in the store
                        if (replicaStatus) {
                            
                            ReplicaClient.ReplicaSetCommand("*3\r\n$3\r\nSET\r\n" + "$" + args[1].length() + "\r\n"
                                    + args[1] + "\r\n" + "$" + args[2].length() + "\r\n" + args[2] + "\r\n");
                        }
                        if (expireAt != null) {
                            expiry.put(key, expireAt); // Set the expiry time for the key
                        } else {
                            expiry.remove(key); // Remove any previous expiry if no expiry specified now
                        }

                        writer.write("+OK\r\n" + key + ":-" + value + "\r\n"); // Send success message and echo
                                                                               // key-value (your existing comment
                                                                               // untouched)
                    }
                    break;

                case "GET":
                    if (args.length != 2) {
                        writer.write("-ERR wrong number of arguments for 'GET'\r\n"); // Error if wrong number of args
                    } else {
                        key = args[1]; // The key to retrieve
                        value = store.get(key); // Get value from store for that key
                        Long expireTime = expiry.get(key); // Check if key has expiry time

                        // If expiry time exists and current time has passed it
                        if (expireTime != null && System.currentTimeMillis() > expireTime) {
                            store.remove(key); // Remove the key-value as it is expired
                            expiry.remove(key); // Remove the expiry time record as well
                            value = null; // Set value to null since key expired
                        }

                        // If value is found and not expired
                        if (value != null) {
                            writer.write("$" + value.length() + "\r\n" + value + "\r\n"); // Send the value as bulk
                            // string
                        } else {
                            writer.write("$-1\r\n"); // Send null bulk string if key does not exist or expired
                        }
                    }
                    break;

                default: // If command is neither SET nor GET
                    writer.write("-ERR unknown command\r\n"); // Inform client that command is not supported
            }

            writer.flush(); // Send all buffered output to client
            client.close(); // Close connection after processing the command

        } catch (IOException e) {
            System.out.println("Handler error: " + e.getMessage()); // Print error message on exceptions
        }
    }

    // A method to start a background thread to clean expired keys periodically
    public static void startExpiryCleanup(ConcurrentHashMap<String, String> store,
            ConcurrentHashMap<String, Long> expiry) {
        Thread cleanupThread = new Thread(() -> {
            while (true) { // Loop forever to keep cleaning
                try {
                    Thread.sleep(60000); // Wait for 60 seconds between each cleanup cycle
                    long now = System.currentTimeMillis(); // Get current system time in milliseconds

                    for (String key : expiry.keySet()) { // Loop through all keys with expiry set
                        Long exp = expiry.get(key); // Get expiry time for the key
                        if (exp != null && now > exp) { // If key is expired
                            store.remove(key); // Remove the key from store
                            expiry.remove(key); // Remove the key from expiry map as well
                        }
                    }
                } catch (InterruptedException e) {
                    break; // Exit loop if thread is interrupted (cleanup stops)
                }
            }
        });

        cleanupThread.setDaemon(true); // Mark thread as daemon so JVM can exit even if running
        cleanupThread.start(); // Start the background cleanup thread
    }
}
