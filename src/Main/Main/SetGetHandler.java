package Main;

// Import required Java libraries for input/output and networking
import java.io.*;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

// Class to handle SET and GET commands in a thread-safe manner
public class SetGetHandler extends Thread {
    // Socket to communicate with client
    private final Socket client;
    // Array to store command arguments
    private final String[] args;
    // Thread-safe map to store key-value pairs
    private final ConcurrentHashMap<String, String> store;
    // Thread-safe map to store expiration times for keys
    private final ConcurrentHashMap<String, Long> expiry;

    // Constructor to initialize the handler with necessary parameters
    public SetGetHandler(Socket client, String[] args, ConcurrentHashMap<String, String> store,
            ConcurrentHashMap<String, Long> expiry) {
        this.client = client;
        this.args = args;
        this.store = store;
        this.expiry = expiry;
    }

    // Method that runs when thread starts
    @Override
    public void run() {
        // Create a writer to send responses to client
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()))) {
            // Convert command to uppercase for case-insensitive comparison
            String command = args[0].toUpperCase();

            // Handle different commands using switch statement
            switch (command) {
                case "SET":
                    // Check if SET command has enough arguments
                    if (args.length < 3) {
                        writer.write("-ERR wrong number of arguments for 'SET'\r\n");
                    }
                    // Extract key and value from arguments
                    String key = args[1];
                    String value = args[2];
                    // Check if key is valid

                    // Optional flags
                    Long expireAt = null; // future timestamp when key should expire
                    boolean nx = false; // only set if key does not exist
                    boolean xx = false; // only set if key already exists

                    // Parse optional arguments (from index 3 onward)
                    for (int i = 3; i < args.length; i++) {
                        String option = args[i].toUpperCase();

                        switch (option) {
                            case "EX": // Expiry in seconds
                                if (i + 1 < args.length) { // here i+1 means the index of next argument
                                    // Check if next argument is a number
                                    int seconds = Integer.parseInt(args[++i]);
                                    expireAt = System.currentTimeMillis() + seconds * 1000L;
                                } else {
                                    writer.write("-ERR syntax error in EX\r\n");
                                    writer.flush();
                                    client.close();
                                    return;
                                }
                                break;

                            case "PX": // Expiry in milliseconds
                                if (i + 1 < args.length) {
                                    int ms = Integer.parseInt(args[++i]);
                                    expireAt = System.currentTimeMillis() + ms;
                                } else {
                                    writer.write("-ERR syntax error in PX\r\n");
                                    writer.flush();
                                    client.close();
                                    return;
                                }
                                break;

                            case "NX":
                                nx = true;
                                break;

                            case "XX":
                                xx = true;
                                break;

                            default:
                                writer.write("-ERR unknown option for SET\r\n");
                                writer.flush();
                                client.close();
                                return;
                        }
                    }
                    // NX -> if key does not exist, set it
                    // XX -> if key exists already,then set it
                    // Check NX/XX conditions
                    boolean keyExists = store.containsKey(key);
                    if ((nx && keyExists) || (xx && !keyExists)) {
                        writer.write("$-1\r\n"); // do nothing
                    } else {
                        store.put(key, value); // set key-value
                        if (expireAt != null) {
                            expiry.put(key, expireAt); // set expiry if provided
                        } else {
                            expiry.remove(key); // remove expiry if none specified
                        }
                        writer.write("+OK\r\n" + key + ":-" + value + "\r\n"); // success response
                    }

                    break;
                case "GET":
                    // Check if GET command has correct number of arguments
                    if (args.length != 2) {
                        writer.write("-ERR wrong number of arguments for 'GET'\r\n");
                    } else {
                        // Get value and check expiry
                        key = args[1];
                        value = store.get(key);
                        Long expireTime = expiry.get(key);
                        // Remove key if expired
                        System.out.println(expireTime + " " + System.currentTimeMillis());
                        if (expireTime != null && System.currentTimeMillis() > expireTime) {
                            store.remove(key);
                            expiry.remove(key);
                            value = null;
                        }
                        // Send response based on value existence
                        if (value != null) {
                            writer.write("$" + value.length() + "\r\n" + value + "\r\n");
                        } else {
                            writer.write("$-1\r\n");
                        }

                    }
                    break;

                default:
                    // Handle unknown commands
                    writer.write("-ERR unknown command\r\n");
            }

            // Ensure all data is sent and close connection
            writer.flush();
            client.close();
        } catch (IOException e) {
            // Print error message if something goes wrong
            System.out.println("Handler error: " + e.getMessage());
        }
    }
}
