package Main;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * ClientHandler is a Thread that manages communication with a single client
 * connected to the server.
 * It parses client commands, executes them, and sends back responses.
 * It also handles replication logic if the server is acting as a master.
 */
public class ClientHandler extends Thread {
    // The socket connection to the client.
    private Socket clientSocket;
    // A thread-safe hash map to store key-value pairs, simulating a data store.
    private ConcurrentHashMap<String, String> store;
    // A thread-safe hash map to store expiration timestamps for keys in the store.
    private ConcurrentHashMap<String, Long> expiry;
    // A thread-safe hash map to store streams
    private ConcurrentHashMap<String, ConcurrentHashMap<String, String>> streams;
    // A boolean indicating whether this server instance is a master.
    private boolean isMaster;
    // A thread-safe list of BufferedWriter objects for all connected replicas.
    // This is used by the master to propagate commands to replicas.
    private CopyOnWriteArrayList<BufferedWriter> connectedReplicasWriters;
    // The replication ID of the master server. Used in replication handshakes.
    private String master_replID;
    // The replication offset of the master server. Used in replication handshakes.
    private int master_repl_offset;
    private ConcurrentHashMap<String, String> lastStreamIds;

    /**
     * Constructor for ClientHandler.
     *
     * @param clientSocket             The socket connected to the client.
     * @param store                    The shared data store.
     * @param expiry                   The shared expiry map for keys.
     * @param isMaster                 A flag indicating if the server is a master.
     * @param connectedReplicasWriters A list of writers to connected replicas (only
     *                                 relevant for master).
     * @param master_replID            The master's replication ID.
     * @param master_repl_offset       The master's replication offset.
     */
    public ClientHandler(
            Socket clientSocket,
            ConcurrentHashMap<String, String> store,
            ConcurrentHashMap<String, Long> expiry,
            ConcurrentHashMap<String, ConcurrentHashMap<String, String>> streams,
            ConcurrentHashMap<String, String> lastStreamIds,
            boolean isMaster,
            CopyOnWriteArrayList<BufferedWriter> connectedReplicasWriters,
            String master_replID,
            int master_repl_offset) {
        this.clientSocket = clientSocket;
        this.store = store;
        this.expiry = expiry;
        this.streams = streams;
        this.lastStreamIds = lastStreamIds;
        this.isMaster = isMaster;
        this.connectedReplicasWriters = connectedReplicasWriters;
        this.master_replID = master_replID;
        this.master_repl_offset = master_repl_offset;
    }

    /**
     * The main execution method for the thread. It continuously reads commands from
     * the client,
     * parses them according to the RESP protocol, and dispatches them to
     * appropriate handlers.
     * It also handles error responses and connection management.
     */
    @Override
    public void run() {
        // Use try-with-resources to ensure reader and writer are closed automatically.
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {

            boolean connectionActive = true;
            // Loop to process commands until the connection is no longer active.
            while (connectionActive) {
                // Read the first line of the command, which should be the array indicator.
                String firstLine = reader.readLine();

                // If the first line is null, the client has disconnected.
                if (firstLine == null) {
                    System.out.println("Client " + clientSocket.getInetAddress() + ":" + clientSocket.getPort()
                            + " disconnected.");
                    connectionActive = false; // Terminate the loop.
                    continue; // Go to the next iteration (which will exit the loop).
                }

                // Validate RESP array protocol: Must start with '*'.
                if (!firstLine.startsWith("*")) {
                    writer.write("-ERR invalid protocol: Expected '*' for array\r\n");
                    writer.flush();
                    connectionActive = false;
                    continue;
                }

                int argsCount;
                try {
                    // Parse the number of arguments from the first line.
                    argsCount = Integer.parseInt(firstLine.substring(1));
                } catch (NumberFormatException e) {
                    writer.write("-ERR invalid protocol: Argument count not a number\r\n");
                    writer.flush();
                    connectionActive = false;
                    continue;
                }

                // Argument count must be positive.
                if (argsCount <= 0) {
                    writer.write("-ERR invalid protocol: Argument count must be positive\r\n");
                    writer.flush();
                    connectionActive = false;
                    continue;
                }

                String[] arguments = new String[argsCount];
                // Loop to read each argument (bulk string) from the client.
                for (int i = 0; i < argsCount; i++) {
                    // Read the bulk string length line.
                    String bulkLenLine = reader.readLine();
                    if (bulkLenLine == null) {
                        writer.write("-ERR invalid protocol: Unexpected end of stream while reading bulk length\r\n");
                        writer.flush();
                        connectionActive = false;
                        break; // Exit the for loop if stream ends unexpectedly.
                    }
                    // Validate RESP bulk string protocol: Must start with '$'.
                    if (!bulkLenLine.startsWith("$")) {
                        writer.write("-ERR invalid protocol: Expected '$' for bulk string length\r\n");
                        writer.flush();
                        connectionActive = false;
                        break;
                    }
                    int bulkLen;
                    try {
                        // Parse the bulk string length.
                        bulkLen = Integer.parseInt(bulkLenLine.substring(1));
                    } catch (NumberFormatException e) {
                        writer.write("-ERR invalid protocol: Bulk string length not a number\r\n");
                        writer.flush();
                        connectionActive = false;
                        break;
                    }

                    // Read the actual bulk string data.
                    char[] buffer = new char[bulkLen];
                    int bytesRead = reader.read(buffer, 0, bulkLen);
                    if (bytesRead != bulkLen) {
                        writer.write("-ERR invalid protocol: Could not read expected bulk string length\r\n");
                        writer.flush();
                        connectionActive = false;
                        break;
                    }
                    arguments[i] = new String(buffer);
                    reader.readLine(); // Consume the trailing "\r\n" after the bulk string.
                }

                // If connection became inactive during argument reading, continue to the next
                // loop iteration (which will exit).
                if (!connectionActive) {
                    continue;
                }

                // Convert the command to uppercase for case-insensitive matching.
                String command = arguments[0].toUpperCase();
                System.out.println("Client " + clientSocket.getInetAddress() + ":" + clientSocket.getPort()
                        + " sent command: " + command);

                // Process the command based on its type.
                switch (command) {
                    case "PING":
                        // Handles PING command.
                        // If PING has an argument, echo it back as a bulk string.
                        // Otherwise, respond with "+PONG".
                        if (arguments.length > 1) {
                            String message = arguments[1];
                            writer.write("$" + message.length() + "\r\n" + message + "\r\n");
                        } else {
                            writer.write("+PONG\r\n");
                        }
                        writer.flush();
                        break;
                    case "ECHO":
                        // Handles ECHO command.
                        // Concatenates all arguments after the command and echoes them back.
                        if (arguments.length >= 2) {
                            StringBuilder sb = new StringBuilder();
                            for (int i = 1; i < arguments.length; i++) {
                                sb.append(arguments[i]).append(" ");
                            }
                            String response = sb.toString().trim();
                            writer.write("$" + response.length() + "\r\n" + response + "\r\n");
                        } else {
                            writer.write("-ERR wrong number of arguments for 'echo' command\r\n");
                        }
                        writer.flush();
                        break;
                    case "SET":
                    case "GET":
                        // Delegates SET and GET commands to a separate handler class.
                        SetGetHandler.handleCommand(arguments, store, expiry, isMaster, connectedReplicasWriters,
                                writer);
                        break;
                    case "CONFIG":
                        // Handles CONFIG GET command for specific parameters.
                        if (arguments.length == 3 && arguments[1].equalsIgnoreCase("GET")) {
                            String param = arguments[2].toLowerCase();
                            String value = null;
                            // Retrieve configuration values from RDBConfig.
                            if (param.equals("dir")) {
                                value = RDBConfig.getDir();
                            } else if (param.equals("dbfilename")) {
                                value = RDBConfig.getDbfilename();
                            }

                            // Respond with a RESP array containing the parameter and its value.
                            if (value != null) {
                                writer.write("*2\r\n");
                                writer.write("$" + param.length() + "\r\n" + param + "\r\n");
                                writer.write("$" + value.length() + "\r\n" + value + "\r\n");
                            } else {
                                writer.write("*0\r\n"); // Empty array if parameter not found.
                            }
                        } else {
                            writer.write("-ERR wrong number of arguments for CONFIG GET\r\n");
                        }
                        writer.flush();
                        break;
                    case "KEYS": {
                        // Handles KEYS * command. Returns all keys in the store.
                        if (arguments.length != 2 || !arguments[1].equals("*")) {
                            writer.write("-ERR only KEYS * is supported\r\n");
                        } else {
                            writer.write("*" + store.size() + "\r\n"); // Array header with number of keys.
                            // Write each key as a bulk string.
                            for (String key : store.keySet()) {
                                writer.write("$" + key.length() + "\r\n" + key + "\r\n");
                            }
                        }
                        writer.flush();
                        break;
                    }
                    case "INFO": {
                        // Handles INFO replication command.
                        if (arguments.length == 2 && arguments[1].equalsIgnoreCase("replication")) {
                            String role = isMaster ? "master" : "slave";
                            String infoString = "role:" + role + "\r\n";
                            // If master, include master replication ID and offset.
                            if (isMaster) {
                                infoString += "master_replid:" + master_replID + "\r\n";
                                infoString += "master_repl_offset:" + master_repl_offset + "\r\n";
                            }
                            // Respond with a bulk string containing the replication info.
                            writer.write("$" + infoString.length() + "\r\n" + infoString + "\r\n");
                        } else {
                            writer.write("-ERR Illegal argument in INFO\r\n");
                        }
                        writer.flush();
                        break;
                    }
                    case "REPLCONF": {
                        // Handles REPLCONF command, typically used by replicas to configure
                        // replication.
                        // If this server is a master and the replica sends "listening-port", add its
                        // writer to the list.
                        if (!isMaster && arguments.length >= 3 && "listening-port".equalsIgnoreCase(arguments[1])) {
                            connectedReplicasWriters.add(writer);
                            System.out.println("Master: New replica connected and added to writers list for port: "
                                    + arguments[2]);
                        } else if (!isMaster && arguments.length >= 3 && "getack".equalsIgnoreCase(arguments[1])) {
                            System.out.println("sending reply for ack cmd");
                            writer.write("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n" + "$"
                                    + ReplicaClient.offset.toString().length() + "\r\n" + ReplicaClient.offset
                                    + "\r\n");
                            writer.flush();
                            break;
                        }
                        writer.write("+OK\r\n"); // Always respond with OK for REPLCONF.
                        writer.flush();
                        break;
                    }
                    case "PSYNC": {
                        // Handles PSYNC command, used for full synchronization by replicas.
                        if (!isMaster) {
                            // If this server is a slave, it doesn't support PSYNC directly from clients.
                            // This scenario might indicate an incorrect setup or a test.
                            // However, in a real scenario, this would be the master responding to a
                            // replica.
                            // For a master, it sends FULLRESYNC and an RDB file.
                            String fullresync = "+FULLRESYNC " + master_replID + " 0\r\n";
                            writer.write(fullresync);
                            writer.flush();

                            // Send an empty RDB file to the replica.
                            OutputStream rawOut = clientSocket.getOutputStream();
                            byte[] rdbBytes = new byte[] {
                                    (byte) 0x52, (byte) 0x45, (byte) 0x44, (byte) 0x49, (byte) 0x53, (byte) 0x30,
                                    (byte) 0x30, (byte) 0x30, (byte) 0x37, // REDIS0007 (RDB magic number and version)
                                    (byte) 0xFA, 0x00, 0x00, 0x00, 0x00, // DB SIZE: 0 key-value pairs
                                    (byte) 0xFF, // EOF marker
                                    (byte) 0x00, 0x00 // CRC64 checksum (placeholders)
                            };
                            String header = "$" + rdbBytes.length + "\r\n";
                            rawOut.write(header.getBytes(StandardCharsets.UTF_8));
                            rawOut.write(rdbBytes);
                            rawOut.flush();
                            System.out.println("Sent empty RDB file to replica.");
                        } else {
                            writer.write("-ERR PSYNC command only supported on master\r\n");
                            writer.flush();
                            connectionActive = false;
                        }
                        break;
                    }
                    case "WAIT": {
                        writer.write(":" + Main.getreplicacount() + "\r\n");
                        writer.flush();
                        break;
                    }
                    case "XADD": {
                        if (arguments.length < 4) { // Check for minimum arguments and odd
                            // number of field-value pairs
                            writer.write("-ERR wrong number of arguments for 'XADD' command\r\n");
                            writer.flush();
                            break;
                        }

                        String streamKey = arguments[1]; // The stream key
                        String newIdString = arguments[2]; // The ID (e.g., "1526919030474-0")

                        String genereatedId = "";
                        // Parse the new ID
                        String[] newIdParts = newIdString.split("-");
                        long newMillisecondsTime;
                        int newSequenceNumber;
                        if (newIdParts[1].equals("*")) {

                            String lastIdString = lastStreamIds.getOrDefault(streamKey, "0-0");
                            String[] lastIdParts = lastIdString.split("-");
                            long lastMillisecondsTime;
                            int lastSequenceNumber;

                            try {
                                lastMillisecondsTime = Long.parseLong(lastIdParts[0]);
                                lastSequenceNumber = Integer.parseInt(lastIdParts[1]);
                            } catch (NumberFormatException e) {
                                // This should ideally not happen if lastStreamIds are always valid
                                // but good for robustness.
                                writer.write("-ERR Internal server error with last ID format\r\n");
                                writer.flush();
                                break;
                            }
                            if (Long.parseLong(newIdParts[0]) == lastMillisecondsTime) {
                                genereatedId = newIdParts[0] + "-" + (lastSequenceNumber + 1);
                                writer.write("$" + genereatedId.length() + "\r\n" + genereatedId + "\r\n");
                            } else {
                                genereatedId = newIdParts[0] + "-" + "0\r\n";
                                writer.write("$" + genereatedId.length() + "\r\n" + genereatedId + "\r\n");
                            }
                            System.out.println(genereatedId);

                            writer.flush();
                        } else {
                            try {
                                newMillisecondsTime = Long.parseLong(newIdParts[0]);
                                newSequenceNumber = Integer.parseInt(newIdParts[1]);
                            } catch (NumberFormatException e) {
                                writer.write("-ERR Invalid ID format\r\n"); // Handle cases where ID parts are not
                                                                            // numbers
                                writer.flush();
                                break;
                            }

                            // Get the last ID for this specific stream
                            String lastIdString = lastStreamIds.getOrDefault(streamKey, "0-0");
                            String[] lastIdParts = lastIdString.split("-");
                            long lastMillisecondsTime;
                            int lastSequenceNumber;

                            try {
                                lastMillisecondsTime = Long.parseLong(lastIdParts[0]);
                                lastSequenceNumber = Integer.parseInt(lastIdParts[1]);
                            } catch (NumberFormatException e) {
                                // This should ideally not happen if lastStreamIds are always valid
                                // but good for robustness.
                                writer.write("-ERR Internal server error with last ID format\r\n");
                                writer.flush();
                                break;
                            }

                            // --- Validation Logic ---

                            // Rule 1: ID must be greater than 0-0
                            if (newMillisecondsTime == 0 && newSequenceNumber == 0) {
                                writer.write("-ERR The ID specified in XADD must be greater than 0-0\r\n");
                                writer.flush();
                                break;
                            }

                            // Rule 2: ID must be greater than the last entry's ID
                            if (newMillisecondsTime < lastMillisecondsTime) {
                                writer.write(
                                        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                                writer.flush();
                                break;
                            } else if (newMillisecondsTime == lastMillisecondsTime) {
                                if (newSequenceNumber <= lastSequenceNumber) {
                                    writer.write(
                                            "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                                    writer.flush();
                                    break;
                                }
                            }
                        }
                        // --- End Validation Logic ---

                        // If validation passes, process the entry
                        // Collect field-value pairs into a map
                        ConcurrentHashMap<String, String> streamData = new ConcurrentHashMap<>();
                        for (int i = 3; i < arguments.length; i += 2) {
                            String field = arguments[i];
                            String value = arguments[i + 1];
                            streamData.put(field, value);
                        }

                        // Store the stream data and update the last ID
                        // Note: The problem statement implies adding to a stream.
                        // For a real Redis stream, you'd append to a list of entries,
                        // not just replace a ConcurrentHashMap for the stream key.
                        // For this stage, simply storing the last entry and updating the last ID might
                        // be sufficient.
                        // If 'streams' is meant to hold the *last* entry's data, your current approach
                        // might be okay for now.
                        // However, for a true stream, 'streams.put(streamKey, streamData)' would
                        // overwrite.
                        // You'd likely need a structure like Map<String, List<Map<String, String>>> or
                        // similar.
                        // For the scope of validating IDs, updating 'lastStreamIds' is key.

                        // Assuming 'streams' is intended to hold the current state for simplicity
                        // based on your original code's `streams.put(streamKey, streamData);`
                        streams.put(streamKey, streamData); // This would overwrite previous entries for the same stream
                                                            // key.
                                                            // A real stream would append. For this problem, let's
                                                            // assume
                                                            // you are only concerned with the *last* ID for validation.

                        // Update the last ID for this stream
                        newIdString = newIdString.endsWith("*") ? genereatedId : newIdString;
                        lastStreamIds.put(streamKey, newIdString);

                        System.out.println(
                                "Added stream: " + streamKey + " with data: " + streamData + " and ID: " + newIdString);
                        writer.write("+" + newIdString + "\r\n"); // Redis usually responds with the ID
                        writer.flush();
                        break;
                    }
                    case "TYPE": {
                        String key = arguments[1];
                        System.out.println(key + " " + store.containsKey(key));
                        if (store.containsKey(key))
                            writer.write("+String\r\n");
                        else if (streams.containsKey(key)) {
                            writer.write("+stream\r\n");
                        } else
                            writer.write("+none\r\n");
                        writer.flush();
                        break;
                    }
                    default:
                        // Handles unknown commands.
                        writer.write("-ERR unknown command '" + command + "'\r\n");
                        writer.flush();
                        connectionActive = false; // Disconnect on unknown commands.
                        break;
                }
            }
        } catch (IOException e) {
            // Handle specific IOExceptions like "Connection reset by peer".
            if (e.getMessage() != null && e.getMessage().contains("Connection reset by peer")) {
                System.out.println("Client " + clientSocket.getInetAddress() + ":" + clientSocket.getPort()
                        + " forcibly closed the connection.");
            } else {
                // Log other IO errors.
                System.err.println("Error handling client " + clientSocket.getInetAddress() + ":"
                        + clientSocket.getPort() + ": " + e.getMessage());
            }
        } finally {
            // Ensure the client socket is closed even if an error occurs.
            try {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                    System.out.println(
                            "Closed client socket for " + clientSocket.getInetAddress() + ":" + clientSocket.getPort());
                }
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
}