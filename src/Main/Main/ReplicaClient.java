package Main;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaClient {
    static ConcurrentHashMap<String, String> ReplicaStore;
    private static ConcurrentHashMap<String, Long> ReplicaExpiry;
    private static Socket masterSocket; // Keep a reference to the master socket
    public static Integer offset = 0;

    public static void connectToMaster(String host, int port, ConcurrentHashMap<String, String> store,
            ConcurrentHashMap<String, Long> expiry) {

        new Thread(() -> {
            ReplicaStore = store;
            ReplicaExpiry = expiry;
            SetGetHandler.startExpiryCleanup(ReplicaStore, ReplicaExpiry); // Ensure cleanup runs

            try {
                masterSocket = new Socket(host, port);
                System.out.println("Connected to master: " + host + ":" + port);

                // Use BufferedWriter for sending commands
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(masterSocket.getOutputStream(), StandardCharsets.UTF_8));
                // Use BufferedReader for receiving command responses
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(masterSocket.getInputStream(), StandardCharsets.UTF_8));

                // Step 1: PING
                String pingCommand = "*1\r\n$4\r\nPING\r\n";
                String response = sendCommand(writer, reader, pingCommand);
                System.out.println("PING response: " + response);
                // Thread.sleep(100);

                if (!"+PONG".equals(response)) { // Note: Removed \r\n as reader.readLine() strips it
                    System.err.println("Unexpected PING response from master: " + response);
                    masterSocket.close();
                    return;
                }
                // Step 2: REPLCONF listening-port
                String replConf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
                response = sendCommand(writer, reader, replConf1);
                System.out.println("REPLCONF listening-port response: " + response);

                if (!"+OK".equals(response)) {
                    System.err.println("Unexpected REPLCONF listening-port response: " + response);
                    masterSocket.close();
                    return;
                }

                // Step 3: REPLCONF capa psync2
                String replConf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                response = sendCommand(writer, reader, replConf2);
                System.out.println("REPLCONF capa psync2 response: " + response);

                if (!"+OK".equals(response)) {
                    System.err.println("Unexpected REPLCONF capa psync2 response: " + response);
                    masterSocket.close();
                    return;
                }

                // Step 4: PSYNC ? -1
                String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                writer.write(psyncCommand); // Send PSYNC
                writer.flush();
                System.out.println("Sending PSYNC: " + psyncCommand.replace("\r\n", "\\r\\n"));

                // Read PSYNC response line (e.g., +FULLRESYNC <replid> <offset>)
                String psyncResponseLine = reader.readLine();
                System.out.println("PSYNC response line: " + psyncResponseLine);

                if (psyncResponseLine != null && psyncResponseLine.startsWith("+FULLRESYNC")) {
                    System.out.println("Full resynchronization initiated.");
                    // After +FULLRESYNC, the master sends an RDB file as a bulk string.
                    // This *must* be read as raw bytes to avoid corruption.
                    readRDBFile(masterSocket.getInputStream());
                    System.out.println("RDB file consumed.");
                    Main.isReplicaReady = true; // Mark replica as ready after RDB
                    readMasterCommands(reader); // Start continuous command reading using the same reader
                } else {
                    System.err.println("Unexpected PSYNC response: " + psyncResponseLine);
                    masterSocket.close();
                }

            } catch (Exception e) {
                System.err.println("Error connecting to master: " + e.getMessage());
                e.printStackTrace();
            } finally {
                // The masterSocket will be closed by readMasterCommands or if an error occurs
                // earlier.
                // No need to close here if readMasterCommands takes over.
                // If an error happens before readMasterCommands, ensure closure.
                if (masterSocket != null && !masterSocket.isClosed()) {
                    try {
                        masterSocket.close();
                    } catch (IOException e) {
                        System.err.println("Error closing master socket: " + e.getMessage());
                    }
                }
            }
        }).start();
    }

    // This method sends a command and reads a single line response
    private static String sendCommand(BufferedWriter writer, BufferedReader reader, String command) throws IOException {
        System.out.println("Sending to master: " + command.replace("\r\n", "\\r\\n"));
        writer.write(command);
        writer.flush();
        String response = reader.readLine();
        System.out.println("send command response:-" + response);
        return response; // Read until \r\n, stripping it
    }

    // This method *must* use InputStream directly for binary RDB data
    private static void readRDBFile(InputStream inputStream) throws IOException {
        // After "+FULLRESYNC <master_replid> <master_repl_offset>\r\n"
        // The next part is a bulk string representing the RDB file.
        // Format: "$<length>\r\n<binary_data>"

        // We need to read the length part using a character reader, then the binary
        // data.
        // Create a temporary BufferedReader just for the length line
        BufferedReader tempReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String dollarLine = tempReader.readLine(); // Read "$<length>" line
        if (dollarLine == null || !dollarLine.startsWith("$")) {
            throw new IOException("Expected '$' for RDB bulk string, but got: " + dollarLine);
        }

        long rdbLength = Long.parseLong(dollarLine.substring(1));
        System.out.println("RDB file length: " + rdbLength + " bytes. Consuming...");

        // Consume the RDB binary data using the raw InputStream
        // It's crucial to read bytes directly for binary data.
        long bytesConsumed = 0;
        byte[] buffer = new byte[4096];
        while (bytesConsumed < rdbLength) {
            int bytesToRead = (int) Math.min(buffer.length, rdbLength - bytesConsumed);
            int read = inputStream.read(buffer, 0, bytesToRead);
            if (read == -1) {
                throw new IOException("Unexpected end of RDB stream.");
            }
            bytesConsumed += read;
        }
        System.out.println("Finished consuming RDB file.");
    }

    // This method continuously reads and processes commands from the master
    private static void readMasterCommands(BufferedReader reader) {
        new Thread(() -> {
            try {
                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        System.out.println("[Master closed connection]");
                        break;
                    }
                    System.out.println("Received from master: " + line.replace("\r\n", "\\r\\n"));

                    if (line.startsWith("*")) { // RESP Array (command)
                        int numArgs = Integer.parseInt(line.substring(1));
                        String[] parsedArgs = new String[numArgs];

                        for (int i = 0; i < numArgs; i++) {
                            String lenLine = reader.readLine(); // e.g., "$3"
                            if (lenLine == null || !lenLine.startsWith("$")) {
                                System.err.println("Invalid RESP format: Expected length line, got " + lenLine);
                                break;
                            }
                            // No need to parse argLength if we just read the whole line as string
                            // int argLength = Integer.parseInt(lenLine.substring(1)); // The length is for
                            // the string that follows

                            parsedArgs[i] = reader.readLine(); // Read the actual argument value
                            offset += parsedArgs[i].getBytes().length + 2;
                            System.out.println("value of offset :-" + offset);
                        }

                        if (parsedArgs.length > 0) {
                            String commandType = parsedArgs[0].toUpperCase();
                            if ("SET".equals(commandType)) {
                                System.out.println("Applying SET command from master: " + Arrays.toString(parsedArgs));
                                ReplicaSetCommand(parsedArgs);
                            } else {
                                System.out.println("Received unsupported command from master: " + commandType);
                                // Potentially send an ACK or handle other master commands
                            }
                        }
                    } else if (line.startsWith("+")) {
                        // Simple string, like +OK. Master might send these for acknowledgments or other
                        // messages.
                        System.out.println("Master simple string: " + line);
                    } else if (line.startsWith("$")) {
                        // This case might occur if a bulk string is sent directly, not as part of an
                        // array.
                        // For replication, SET commands are usually arrays.
                        // For simplicity, we'll assume SETs are always in arrays for now.
                        System.out.println("Master bulk string length line: " + line);
                        reader.readLine(); // Consume the actual bulk string value
                    } else {
                        System.out.println("Unknown line from master: " + line);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading commands from master: " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    if (masterSocket != null && !masterSocket.isClosed()) {
                        masterSocket.close();
                    }
                } catch (IOException e) {
                    System.err.println("Error closing master socket in command reader: " + e.getMessage());
                }
            }
        }).start();
    }

    public static void ReplicaSetCommand(String... args) {
        if (args.length < 3) {
            System.err.println("Invalid SET command received from master: " + Arrays.toString(args));
            return;
        }

        String key = args[1];
        String value = args[2];
        Long expireAt = null;

        // Parse optional arguments (EX, PX)
        for (int i = 3; i < args.length; i++) {
            String option = args[i].toUpperCase();
            switch (option) {
                case "EX":
                    if (i + 1 < args.length) {
                        try {
                            int seconds = Integer.parseInt(args[++i]);
                            expireAt = System.currentTimeMillis() + seconds * 1000L;
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid EX seconds value: " + args[i]);
                        }
                    }
                    break;
                case "PX":
                    if (i + 1 < args.length) {
                        try {
                            int ms = Integer.parseInt(args[++i]);
                            expireAt = System.currentTimeMillis() + ms;
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid PX milliseconds value: " + args[i]);
                        }
                    }
                    break;
            }
        }

        ReplicaStore.put(key, value);
        if (expireAt != null) {
            ReplicaExpiry.put(key, expireAt);
        } else {
            ReplicaExpiry.remove(key); // Remove any expiry if not specified
        }
        System.out.println("Replica applied SET: " + key + " = " + value
                + (expireAt != null ? " (expires at " + expireAt + ")" : ""));
    }

    // Helper to encode commands into RESP format (can be moved to a utility class)
    public static String encodeRESPCommand(String... args) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            sb.append("$").append(arg.length()).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        return sb.toString();
    }
}