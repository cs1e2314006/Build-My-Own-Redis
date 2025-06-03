package Main;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter; // Needed for cleanup thread, if it writes logs
import java.util.Iterator; // For safe removal during iteration
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

// This class handles the SET and GET commands, now as a static helper.
// It does NOT manage the client socket lifecycle.
public class SetGetHandler {

    // Handles a single SET or GET command for a connected client.
    // It takes the BufferedWriter for the client's output stream directly.
    public static void handleCommand(
            String[] args, // The command arguments from the client
            ConcurrentHashMap<String, String> store,
            ConcurrentHashMap<String, Long> expiry,
            boolean isMaster, // Indicates if this server instance is a master
            CopyOnWriteArrayList<BufferedWriter> connectedReplicasWriters, // List of replica output streams
            BufferedWriter clientWriter // The writer connected to the current client
    ) throws IOException { // Throws IOException, letting ClientHandler handle it
        String command = args[0].toUpperCase();
        System.out.println("isMaster status for command '" + command + "': " + isMaster);

        switch (command) {
            case "SET":
                if (args.length < 3) {
                    clientWriter.write("-ERR wrong number of arguments for 'SET'\r\n");
                    clientWriter.flush();
                    return;
                }

                String key = args[1];
                String value = args[2];
                Long expireAt = null;
                boolean nx = false;
                boolean xx = false;

                for (int i = 3; i < args.length; i++) {
                    String option = args[i].toUpperCase();
                    switch (option) {
                        case "EX":
                            if (i + 1 < args.length) {
                                try {
                                    int seconds = Integer.parseInt(args[++i]);
                                    expireAt = System.currentTimeMillis() + seconds * 1000L;
                                } catch (NumberFormatException e) {
                                    clientWriter.write("-ERR value is not an integer or out of range\r\n");
                                    clientWriter.flush();
                                    return;
                                }
                            } else {
                                clientWriter.write("-ERR syntax error\r\n");
                                clientWriter.flush();
                                return;
                            }
                            break;
                        case "PX":
                            if (i + 1 < args.length) {
                                try {
                                    int ms = Integer.parseInt(args[++i]);
                                    expireAt = System.currentTimeMillis() + ms;
                                } catch (NumberFormatException e) {
                                    clientWriter.write("-ERR value is not an integer or out of range\r\n");
                                    clientWriter.flush();
                                    return;
                                }
                            } else {
                                clientWriter.write("-ERR syntax error\r\n");
                                clientWriter.flush();
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
                            clientWriter.write("-ERR unknown option for SET\r\n");
                            clientWriter.flush();
                            return;
                    }
                }

                if (nx && xx) {
                    clientWriter.write("-ERR NX and XX options at the same time are not allowed\r\n");
                    clientWriter.flush();
                    return;
                }

                boolean keyExists = store.containsKey(key);
                boolean performedSet = false;

                if ((nx && keyExists) || (xx && !keyExists)) {
                    clientWriter.write("$-1\r\n"); // Respond with null bulk string indicating no operation done
                } else {
                    store.put(key, value);
                    if (expireAt != null) {
                        expiry.put(key, expireAt);
                        System.out.println("Set key: " + key + ", value: " + value + ", expires in "
                                + (expireAt - System.currentTimeMillis()) + "ms");
                    } else {
                        expiry.remove(key); // Ensure no old expiry is left
                        System.out.println("Set key: " + key + ", value: " + value);
                    }
                    clientWriter.write("+OK\r\n"); // Send success message
                    performedSet = true;
                }

                // *** REPLICATION LOGIC FOR MASTER ***
                if (!isMaster && performedSet) {
                    // Construct the RESP command to send to replicas
                    String replicaCommand;
                    System.out.println("inside this block");
                    // For propagation, always send the full original command including options
                    StringBuilder replicaCmdBuilder = new StringBuilder();
                    replicaCmdBuilder.append("*").append(args.length).append("\r\n");
                    for (String arg : args) {
                        replicaCmdBuilder.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
                    }
                    replicaCommand = replicaCmdBuilder.toString();
                    propagateCommandToReplicas(replicaCommand, connectedReplicasWriters);
                }
                break;

            case "GET":
                if (args.length != 2) {
                    clientWriter.write("-ERR wrong number of arguments for 'GET'\r\n");
                } else {
                    key = args[1];
                    value = store.get(key);
                    Long expireTime = expiry.get(key);

                    if (expireTime != null && System.currentTimeMillis() > expireTime) {
                        store.remove(key);
                        expiry.remove(key);
                        value = null; // Mark as expired
                    }

                    if (value != null) {
                        clientWriter.write("$" + value.length() + "\r\n" + value + "\r\n");
                    } else {
                        clientWriter.write("$-1\r\n");
                    }
                }
                break;

            default:
                // This case should ideally not be reached if commands are filtered in
                // ClientHandler
                clientWriter.write("-ERR unknown command\r\n");
        }
        clientWriter.flush(); // Always flush after sending a response
    }

    // This method starts a background thread to clean up expired keys.
    // It should be called once from Main.
    public static void startExpiryCleanup(ConcurrentHashMap<String, String> store,
            ConcurrentHashMap<String, Long> expiry) {
        Thread cleanupThread = new Thread(() -> {
            while (true) {
                try {
                    // Check more frequently than 60 seconds for better responsiveness
                    Thread.sleep(100); // Check every 100 milliseconds

                    long now = System.currentTimeMillis();

                    // Using an iterator to safely remove elements during iteration
                    Iterator<String> keyIterator = expiry.keySet().iterator();
                    while (keyIterator.hasNext()) {
                        String key = keyIterator.next();
                        Long exp = expiry.get(key);
                        if (exp != null && now > exp) {
                            store.remove(key);
                            keyIterator.remove(); // Safely remove from expiry map
                            System.out.println("Expired key removed by cleanup: " + key);
                        }
                    }
                } catch (InterruptedException e) {
                    System.err.println("Expiry cleanup thread interrupted: " + e.getMessage());
                    break; // Exit the loop if thread is interrupted
                } catch (Exception e) {
                    System.err.println("Error in expiry cleanup thread: " + e.getMessage());
                }
            }
        });

        cleanupThread.setDaemon(true); // Allow JVM to exit if only daemon threads remain
        cleanupThread.setName("ExpiryCleanupThread");
        cleanupThread.start();
    }

    // Method to propagate commands to all connected replicas
    // It is static because it's called from a static context (handleCommand)
    private static void propagateCommandToReplicas(String command, CopyOnWriteArrayList<BufferedWriter> replicas) {
        System.out.println("Propagating to replica: " + command.replace("\r\n", "\\r\\n").trim());

        // Use a standard for-each loop and remove directly from the list if there's an
        // error.
        // CopyOnWriteArrayList ensures thread-safety for iteration and modification.
        for (BufferedWriter writer : replicas) {
            try {
                writer.write(command);
                writer.flush();
                System.out.println("Successfully propagated command to a replica.");
            } catch (IOException e) {
                System.err.println("Error propagating command to replica. Removing writer: " + e.getMessage());
                // Remove the disconnected writer directly from the CopyOnWriteArrayList.
                // This is safe and won't cause ConcurrentModificationException.
                // The current iteration might still process other elements based on the
                // list's state at the beginning of the iteration, but subsequent iterations
                // will reflect the change.
                replicas.remove(writer);
            }
        }
    }
}
