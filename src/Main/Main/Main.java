package Main;

// Import necessary classes for networking and input/output operations
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import Main.ReplicaClient;

// Import the RDBConfig class that handles directory and filename settings for RDB
// checked for RDB support and key loading and tested for multiple key and string values
@SuppressWarnings("unused")
public class Main {

    // A shared key-value store that allows multiple threads to safely read and
    // write data at the same time.
    private static final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();
    // for checking replica or master
    private static boolean isReplica = false;
    private static String master_replID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static int master_repl_offset = 0;
    private static boolean isReplicaReady = false;

    public static void main(String[] args) throws Exception {
        // === PARSE RDB CONFIGURATION FROM COMMAND LINE (added for RDB support) ===
        RDBConfig.parseArguments(args); // Delegated RDB config parsing to a separate class
        // Load keys from RDB file into the store before starting the server
        RDBKeyHandler.loadRdbFile(RDBConfig.getDir(), RDBConfig.getDbfilename(), store);
        System.out.println(args.length);

        SetGetHandler.startExpiryCleanup(store, expiry);
        // Display a message indicating that the server has started
        System.out.println("Server started");

        // Define the port number where the server will listen for connections
        int Masterport = 6379;
        int slaveport = -1;
        String MasterHost = null;
        // Check for --port argument
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port") && i + 1 < args.length) {
                try {
                    Masterport = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid port number: " + args[i + 1]);
                    return;
                }

            } else if (args[i].equals("--replicaof") && i + 1 < args.length) {
                isReplica = true;
                slaveport = Masterport;// putting previously accessed port to slave port as there is replica flag
                // present in command
                MasterHost = args[++i];
                try {
                    Masterport = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid port number: " + args[i + 1]);
                    return;
                }

            }

        }
        System.out.println(MasterHost + " " + Masterport + " " + slaveport);
        if (isReplica) {
            ReplicaClient.connectToMaster(MasterHost, Masterport, store, expiry);
        }
        // Start a try-with-resources block that automatically closes the server socket
        // when done
        try (ServerSocket serverSocket = new ServerSocket(Masterport)) {
            // Allow the port to be reused quickly after the server is restarted
            serverSocket.setReuseAddress(true);
            // Keep the server running continuously to accept client connections
            while (true) {
                // Wait for a client to connect; once connected, a socket is created
                Socket client = serverSocket.accept();
                System.out.println("Client connected.");

                // Set up a reader to read input (commands) sent by the client
                BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));

                // Read the first line from the client, which should start the RESP command
                String firstLine = reader.readLine();

                // If the client sends nothing or disconnects, close the connection and skip
                if (firstLine == null) {
                    client.close();
                    continue;
                }

                // RESP (Redis Serialization Protocol) commands should start with '*'
                if (firstLine.startsWith("*")) {
                    // Extract how many arguments the command has (e.g., "*3" means 3 parts)
                    int argsCount = Integer.parseInt(firstLine.substring(1));

                    // Create an array to store each part of the command (like command name, key,
                    // value)
                    String[] arguments = new String[argsCount];

                    // Loop through each argument in RESP format:
                    // RESP sends each value in two lines — first the length (which we ignore), then
                    // the actual value
                    for (int i = 0; i < argsCount; i++) {
                        reader.readLine(); // Skip the line that gives the length (e.g., "$3")
                        arguments[i] = reader.readLine(); // Read the actual value (e.g., "SET", "key", "value")
                        System.out.println("Argument " + i + ": " + arguments[i]); // Print each argument for debugging
                    }

                    // Convert the first argument (command name) to uppercase to make comparison
                    // easier
                    String command = arguments[0].toUpperCase();
                    System.out.println("Command: " + command); // Print which command was received

                    // Check which command the client sent and handle it appropriately

                    switch (command) {
                        case "PING":
                            // If command is PING, start a thread that handles pinging
                            new MultiplePings(client, arguments).start();
                            break;

                        case "ECHO":
                            // If command is ECHO, start a thread that echoes back the message
                            new Echo(client, arguments).start();
                            break;

                        case "SET":
                        case "GET":
                            // If command is SET or GET, start a thread to read/write from the shared
                            // key-value store
                            new SetGetHandler(client, arguments, store, expiry, isReplicaReady).start();
                            break;

                        // === CONFIG GET HANDLING (added for RDB support) ===
                        case "CONFIG":
                            // Only handle CONFIG GET for "dir" and "dbfilename"
                            if (arguments.length == 3 && arguments[1].equalsIgnoreCase("GET")) {
                                String param = arguments[2].toLowerCase();
                                String value = null;

                                // Use getters from the RDBConfig class to fetch configuration values
                                if (param.equals("dir")) {
                                    value = RDBConfig.getDir();
                                } else if (param.equals("dbfilename")) {
                                    value = RDBConfig.getDbfilename();
                                }

                                BufferedWriter writer = new BufferedWriter(
                                        new OutputStreamWriter(client.getOutputStream()));
                                if (value != null) {
                                    // RESP array: *2\r\n$<len>\r\n<param>\r\n$<len>\r\n<value>\r\n
                                    writer.write("*2\r\n");
                                    writer.write("$" + param.length() + "\r\n" + param + "\r\n");
                                    writer.write("$" + value.length() + "\r\n" + value + "\r\n");
                                } else {
                                    // If param not found, return empty array
                                    writer.write("*0\r\n");
                                }
                                writer.flush();
                                client.close();
                            } else {
                                BufferedWriter writer = new BufferedWriter(
                                        new OutputStreamWriter(client.getOutputStream()));
                                writer.write("-ERR wrong number of arguments for CONFIG GET\r\n");
                                writer.flush();
                                client.close();
                            }
                            break;
                        // === END CONFIG GET HANDLING ===

                        case "KEYS": {
                            BufferedWriter writer = new BufferedWriter(
                                    new OutputStreamWriter(client.getOutputStream()));
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

                            BufferedWriter writer = new BufferedWriter(
                                    new OutputStreamWriter(client.getOutputStream()));
                            if (arguments.length == 2 && arguments[1].equalsIgnoreCase("replication")) {
                                String role = isReplica ? "slave" : "master";
                                writer.write("--Information about server--\r\n");
                                writer.write("$" + ("role:" + role).length() + "\r\n" + "role:" + role + "\r\n");
                                int masterlen = master_replID.length() + 13;
                                writer.write("$" + masterlen + "\r\n" + "master_replID:-"
                                        + master_replID + "\r\n");
                                writer.write("$" + 19 + "\r\n" + "master_repl_offset:-" + master_repl_offset + "\r\n");
                            } else {
                                writer.write("-ERR Illegal argument in INFO\r\n");
                            }
                            writer.flush();
                            client.close();
                            break;
                        }
                        case "REPLCONF": {
                            BufferedWriter writer = new BufferedWriter(
                                    new OutputStreamWriter(client.getOutputStream()));
                            writer.write("+OK\r\n");
                            writer.flush();
                            client.close();
                            break;
                        }
                        case "PSYNC": {
                            BufferedWriter writer = new BufferedWriter(
                                    new OutputStreamWriter(client.getOutputStream()));
                            String fullresync = "+FULLRESYNC " + master_replID + " 0\r\n";
                            writer.write(fullresync);
                            writer.flush();
                            // as buffer writer is only capable of sending character,strings etc.
                            // but when you send some thing related to byte,int or byte array then it will
                            // corrupt your file(due to encoding like UTF-8, buffering, or line
                            // conversions).
                            OutputStream rawOut = client.getOutputStream();
                            // the content of empty rdb file in hex format
                            byte[] rdbBytes = new byte[] {
                                    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x37,
                                    (byte) 0xFA, 0x00, 0x00, 0x00, 0x00,
                                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                                    0x00, 0x00
                            };

                            String header = "$" + rdbBytes.length + "\r\n";
                            rawOut.write(header.getBytes()); // write header
                            rawOut.write(rdbBytes); // write binary RDB file
                            rawOut.flush();
                            isReplicaReady = true;
                            client.close();
                            break;
                        }

                        default:
                            // If the command is not recognized, send back an error message to the client
                            BufferedWriter writer = new BufferedWriter(
                                    new OutputStreamWriter(client.getOutputStream()));
                            writer.write("-ERR unknown command\r\n");
                            writer.flush(); // Send the error immediately
                            client.close(); // Close the connection
                    }

                } else {
                    // If the message does not follow RESP format, inform the client it's invalid
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
                    writer.write("-ERR invalid protocol\r\n");
                    writer.flush();
                    client.close(); // Close the connection because it’s not valid
                }
            }

        } catch (IOException e) {
            // Catch and display any error that occurs while the server is running
            System.out.println("Server Error: " + e.getMessage());
        }
    }
}
