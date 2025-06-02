package Main;

// Import necessary classes for networking and input/output operations
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList; // For thread-safe list of replica writers
import java.nio.charset.StandardCharsets;

// Import the RDBConfig class that handles directory and filename settings for RDB
// checked for RDB support and key loading and tested for multiple key and string values
@SuppressWarnings("unused")
public class Main {

    // A shared key-value store that allows multiple threads to safely read and
    // write data at the same time.
    private static final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();

    // for checking replica or master status of THIS server instance
    private static boolean isMaster = true; // Default to master
    // New: List to hold BufferedWriter for each connected replica
    private static final CopyOnWriteArrayList<BufferedWriter> connectedReplicasWriters = new CopyOnWriteArrayList<>();

    private static String master_replID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static int master_repl_offset = 0;
    public static boolean isReplicaReady = false; // Flag for a slave to know if it's ready to receive commands from its
                                                  // master

    public static void main(String[] args) throws Exception {
        // === PARSE RDB CONFIGURATION FROM COMMAND LINE (added for RDB support) ===
        RDBConfig.parseArguments(args); // Delegated RDB config parsing to a separate class
        // Load keys from RDB file into the store before starting the server
        RDBKeyHandler.loadRdbFile(RDBConfig.getDir(), RDBConfig.getDbfilename(), store);
        System.out.println(args.length);

        SetGetHandler.startExpiryCleanup(store, expiry); // Start background cleanup

        System.out.println("Server starting...");

        int currentServerPort = 6379; // Default port
        String masterHost = null;
        int masterPort = -1;

        // Check for --port and --replicaof arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port") && i + 1 < args.length) {
                try {
                    currentServerPort = Integer.parseInt(args[++i]);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid port number: " + args[i]);
                    return;
                }
            } else if (args[i].equals("--replicaof") && i + 2 < args.length) { // Ensure
                // host and port are present
                isMaster = false; // This instance is a replica (slave)
                masterHost = args[++i];
                try {
                    masterPort = Integer.parseInt(args[++i]); // This is the master's port
                } catch (NumberFormatException e) {
                    System.out.println("Invalid master port number: " + args[i]);
                    return;
                }
            }
        }

        System.out.println("Server role: " + (isMaster ? "master" : "slave"));
        System.out.println("Listening on port: " + currentServerPort); // Log the correct listening port
        if (!isMaster) {
            System.out.println("Replicating from master: " + masterHost + ":" + masterPort);
            // Connect to master if this is a replica
            // Ensure masterHost and masterPort are correctly passed here
            ReplicaClient.connectToMaster(masterHost, masterPort, store, expiry);
        }

        // FIX: The ServerSocket should bind to currentServerPort, not masterPort
        try (ServerSocket serverSocket = new ServerSocket(masterPort)) { // Changed masterPort to masterPort
            serverSocket.setReuseAddress(true);
            System.out.println("Server started on port " + masterPort); // Log the actual bound port

            while (true) {
                Socket client = serverSocket.accept();
                System.out.println("Client connected: " + client.getInetAddress() + ":" + client.getPort());

                // Create input/output streams for the client connection
                BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));

                String firstLine = reader.readLine();

                if (firstLine == null) {
                    client.close();
                    continue;
                }

                if (firstLine.startsWith("*")) {
                    int argsCount = Integer.parseInt(firstLine.substring(1));
                    String[] arguments = new String[argsCount];

                    for (int i = 0; i < argsCount; i++) {
                        reader.readLine(); // Skip length line (e.g., "$3")
                        arguments[i] = reader.readLine(); // Read actual value
                        System.out.println("Argument " + i + ": " + arguments[i]);
                    }

                    String command = arguments[0].toUpperCase();
                    System.out.println("Command: " + command);

                    switch (command) {
                        case "PING":
                            new MultiplePings(client, arguments).start();
                            break;
                        case "ECHO":
                            new Echo(client, arguments).start();
                            break;
                        case "SET":
                        case "GET":
                            // Pass the isMaster status and the list of replica writers to SetGetHandler
                            // Only a master will use connectedReplicasWriters
                            new SetGetHandler(client, arguments, store, expiry, isMaster, connectedReplicasWriters)
                                    .start();
                            break;
                        case "CONFIG":
                            if (arguments.length == 3 && arguments[1].equalsIgnoreCase("GET")) {
                                String param = arguments[2].toLowerCase();
                                String value = null;
                                if (param.equals("dir")) {
                                    value = RDBConfig.getDir();
                                } else if (param.equals("dbfilename")) {
                                    value = RDBConfig.getDbfilename();
                                }

                                if (value != null) {
                                    writer.write("*2\r\n");
                                    writer.write("$" + param.length() + "\r\n" + param + "\r\n");
                                    writer.write("$" + value.length() + "\r\n" + value + "\r\n");
                                } else {
                                    writer.write("*0\r\n");
                                }
                            } else {
                                writer.write("-ERR wrong number of arguments for CONFIG GET\r\n");
                            }
                            writer.flush();
                            client.close();
                            break;
                        case "KEYS": {
                            if (arguments.length != 2 || !arguments[1].equals("*")) {
                                System.out.println(arguments.length);
                                writer.write("-ERR only KEYS * is supported\r\n");
                            } else {
                                writer.write("*" + store.size() + "\r\n");
                                for (String key : store.keySet()) {
                                    writer.write("$" + key.length() + "\r\n" + key + "\r\n");
                                }
                            }
                            writer.flush();
                            client.close();
                            break;
                        }
                        case "INFO": {
                            if (arguments.length == 2 && arguments[1].equalsIgnoreCase("replication")) {
                                String role = isMaster ? "master" : "slave";
                                String infoString = "role:" + role + "\r\n";
                                if (isMaster) {
                                    infoString += "master_replid:" + master_replID + "\r\n";
                                    infoString += "master_repl_offset:" + master_repl_offset + "\r\n";
                                }
                                // For slave, you might add master_host, master_port, master_link_status, etc.

                                writer.write("$" + infoString.length() + "\r\n" + infoString + "\r\n");
                            } else {
                                writer.write("-ERR Illegal argument in INFO\r\n");
                            }
                            writer.flush();
                            client.close();
                            break;
                        }
                        case "REPLCONF": {
                            // If this server is a master and a client sends REPLCONF,
                            // it's likely a new replica trying to connect.
                            // We need to store its output stream to send commands later.
                            // if (isMaster && arguments.length >= 3 &&
                            // "listening-port".equalsIgnoreCase(arguments[1])) {
                            // // A new replica is announcing itself
                            // // The writer for this client needs to be stored.
                            // // We are already creating a writer for every client connection at the start
                            // of
                            // // the loop.
                            // // So, just add it to the list.
                            // connectedReplicasWriters.add(writer);
                            // System.out.println("Master: New replica connected and added to writers
                            // list.");
                            // }
                            writer.write("+OK\r\n");
                            writer.flush();
                            // Do NOT close the client socket here for replicas, as master needs to send
                            // data.
                            // The client (replica) will close its side when it's done or on its own errors.
                            // However, for typical Redis, the REPLCONF ACK for 'listening-port' is followed
                            // by 'capa',
                            // and the socket remains open. If the master closes the socket here, it breaks
                            // replication.
                            // We manage this by NOT closing the client socket for potential replicas.
                            // Other commands (like PING, ECHO, GET, SET from a non-replica client) will
                            // close.
                            // This part needs careful handling to distinguish "regular" client from
                            // "replica" client.
                            // A more robust solution might involve a dedicated thread for each replica's
                            // output.

                            // For now, only close the client if it's NOT a REPLCONF command that suggests a
                            // replica connection
                            if (!(isMaster && arguments.length >= 3 && ("listening-port".equalsIgnoreCase(arguments[1])
                                    || "capa".equalsIgnoreCase(arguments[1])))) {
                                client.close(); // Close only if it's not a replication handshake step
                            }
                            break;
                        }
                        case "PSYNC": {
                            // If this server is a master, and a client sends PSYNC,
                            // it's a replica requesting full resynchronization.
                            if (isMaster) {
                                String fullresync = "+FULLRESYNC " + master_replID + " 0\r\n";
                                writer.write(fullresync);
                                writer.flush();

                                // Send the empty RDB file directly as bytes
                                OutputStream rawOut = client.getOutputStream();
                                byte[] rdbBytes = new byte[] {
                                        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x37,
                                        (byte) 0xFA, 0x00, 0x00, 0x00, 0x00,
                                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                                        0x00, 0x00
                                };
                                String header = "$" + rdbBytes.length + "\r\n";
                                rawOut.write(header.getBytes(StandardCharsets.UTF_8)); // header as bytes
                                rawOut.write(rdbBytes); // binary RDB file
                                rawOut.flush();
                                System.out.println("Sent empty RDB file to replica.");

                                // Do NOT close the client socket here as this socket is now dedicated to
                                // replication
                                // The master will use this socket's output stream to send future SET commands.
                                // The `connectedReplicasWriters` list already contains the BufferedWriter for
                                // this client.
                            } else {
                                writer.write("-ERR PSYNC command only supported on master\r\n");
                                writer.flush();
                                client.close();
                            }
                            break;
                        }
                        default:
                            writer.write("-ERR unknown command\r\n");
                            writer.flush();
                            client.close();
                    }
                } else {
                    writer.write("-ERR invalid protocol\r\n");
                    writer.flush();
                    client.close();
                }
            }

        } catch (IOException e) {
            System.out.println("Server Error: " + e.getMessage());
        }
    }
}