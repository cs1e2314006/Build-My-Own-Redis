package Main;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList; // For thread-safe list of replica writers

// This class handles the SET and GET commands from clients, runs in its own thread
public class SetGetHandler extends Thread {
    private final Socket client;
    private final String[] args;
    private final ConcurrentHashMap<String, String> store;
    private final ConcurrentHashMap<String, Long> expiry;
    private final boolean isMaster; // Indicates if this server instance is a master
    // New: List of replica output streams for propagation
    private final CopyOnWriteArrayList<BufferedWriter> connectedReplicasWriters;

    // Constructor to create a handler for a client connection with provided command
    // arguments, data maps, and master status, and replica writers
    public SetGetHandler(Socket client, String[] args, ConcurrentHashMap<String, String> store,
            ConcurrentHashMap<String, Long> expiry, boolean isMaster,
            CopyOnWriteArrayList<BufferedWriter> connectedReplicasWriters) {
        this.client = client;
        this.args = args;
        this.store = store;
        this.expiry = expiry;
        this.isMaster = isMaster; // Set the master status
        this.connectedReplicasWriters = connectedReplicasWriters; // Store the list of replica writers
    }

    @Override
    public void run() {
        try (BufferedWriter clientWriter = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()))) {
            String command = args[0].toUpperCase();
            System.out.println("isMaster status for command '" + command + "': " + isMaster);

            switch (command) {
                case "SET":
                    if (args.length < 3) {
                        clientWriter.write("-ERR wrong number of arguments for 'SET'\r\n");
                        clientWriter.flush();
                        client.close();
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
                                        client.close();
                                        return;
                                    }
                                } else {
                                    clientWriter.write("-ERR syntax error\r\n");
                                    clientWriter.flush();
                                    client.close();
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
                                        client.close();
                                        return;
                                    }
                                } else {
                                    clientWriter.write("-ERR syntax error\r\n");
                                    clientWriter.flush();
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
                                clientWriter.write("-ERR unknown option for SET\r\n");
                                clientWriter.flush();
                                client.close();
                                return;
                        }
                    }

                    if (nx && xx) {
                        clientWriter.write("-ERR NX and XX options at the same time are not allowed\r\n");
                        clientWriter.flush();
                        client.close();
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
                        } else {
                            expiry.remove(key);
                        }
                        clientWriter.write("+OK\r\n"); // Send success message
                        performedSet = true;
                    }

                    // *** REPLICATION LOGIC FOR MASTER ***
                    if (isMaster && performedSet) {
                        // Construct the RESP command to send to replicas
                        String replicaCommand;
                        if (expireAt != null) {
                            // Include EX or PX based on original command's type. For simplicity, we just
                            // use PX for all
                            // propagated expiry, converting EX seconds to milliseconds.
                            long originalExpiryMs = expireAt; // Already calculated in milliseconds
                            replicaCommand = ReplicaClient.encodeRESPCommand("SET", key, value, "PX",
                                    String.valueOf(originalExpiryMs - System.currentTimeMillis()));
                        } else {
                            replicaCommand = ReplicaClient.encodeRESPCommand("SET", key, value);
                        }
                        propagateCommandToReplicas(replicaCommand);
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
                            value = null;
                        }

                        if (value != null) {
                            clientWriter.write("$" + value.length() + "\r\n" + value + "\r\n");
                        } else {
                            clientWriter.write("$-1\r\n");
                        }
                    }
                    break;

                default:
                    clientWriter.write("-ERR unknown command\r\n");
            }

            clientWriter.flush();
            client.close();

        } catch (IOException e) {
            System.out.println("Handler error: " + e.getMessage());
        }
    }

    // New: Method to propagate commands to all connected replicas
    private void propagateCommandToReplicas(String command) {
        // Use an iterator to safely remove disconnected replicas
        connectedReplicasWriters.forEach(writer -> {
            try {
                System.out.println("Propagating to replica: " + command.replace("\r\n", "\\r\\n"));
                writer.write(command);
                writer.flush();
            } catch (IOException e) {
                System.err.println("Error propagating command to replica: " + e.getMessage() + ". Removing writer.");
                // Remove the disconnected writer
                // Note: Removing within forEach might lead to ConcurrentModificationException
                // CopyOnWriteArrayList handles this gracefully, but iterating with an explicit
                // Iterator and `remove()` is often safer for other list types.
                // For CopyOnWriteArrayList, it's safe to modify during iteration, but the
                // element
                // might still be processed in the current iteration.
                // A more robust approach would be to add to a "to-remove" list and clean up
                // after the loop.
            }
        });
        // A more robust cleanup could be done here if needed, or periodically in Main.
        // For simplicity with CopyOnWriteArrayList, we let failed writes indicate
        // removal.
    }

    public static void startExpiryCleanup(ConcurrentHashMap<String, String> store,
            ConcurrentHashMap<String, Long> expiry) {
        Thread cleanupThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(60000);
                    long now = System.currentTimeMillis();

                    for (String key : expiry.keySet()) {
                        Long exp = expiry.get(key);
                        if (exp != null && now > exp) {
                            store.remove(key);
                            expiry.remove(key);
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });

        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }
}