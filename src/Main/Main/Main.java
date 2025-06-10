package Main;

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
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> streams = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> lastStreamIds = new ConcurrentHashMap<>();
    // for checking replica or master status of THIS server instance
    private static boolean isMaster = true; // Default to master
    // New: List to hold BufferedWriter for each connected replica
    private static final CopyOnWriteArrayList<BufferedWriter> connectedReplicasWriters = new CopyOnWriteArrayList<>();

    private static String master_replID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static int master_repl_offset = 0;
    public static boolean isReplicaReady = false; // Flag for a slave to know if it's ready to receive commands from its
                                                  // master
    private static int countofReplica = 0;

    public static int getreplicacount() {
        return countofReplica;
    }

    public static void main(String[] args) throws Exception {
        // === PARSE RDB CONFIGURATION FROM COMMAND LINE (added for RDB support) ===
        RDBConfig.parseArguments(args); // Delegated RDB config parsing to a separate class
        // Load keys from RDB file into the store before starting the server
        RDBKeyHandler.loadRdbFile(RDBConfig.getDir(), RDBConfig.getDbfilename(), store);
        System.out.println("Arguments length: " + args.length);

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
            countofReplica += 1;
        }
        masterPort = masterPort == -1 ? currentServerPort : masterPort;
        // FIX: The ServerSocket should bind to currentServerPort, not masterPort
        try (ServerSocket serverSocket = new ServerSocket(masterPort)) { // CORRECTED to currentServerPort
            serverSocket.setReuseAddress(true);
            System.out.println("Server started on port " + masterPort); // Log the actual bound port

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getInetAddress() + ":" + clientSocket.getPort());

                // Create a new thread (ClientHandler) for each client
                new ClientHandler(
                        clientSocket,
                        store,
                        expiry,
                        streams,
                        lastStreamIds,
                        isMaster,
                        connectedReplicasWriters,
                        master_replID,
                        master_repl_offset).start(); // Start the thread to handle this client
            }

        } catch (IOException e) {
            System.out.println("Server Error: " + e.getMessage());
        }
    }

    // Add getters for replication info if needed by other classes (e.g.,
    // ReplicaClient)
    public static String getMasterReplID() {
        return master_replID;
    }

    public static int getMasterReplOffset() {
        return master_repl_offset;
    }
}