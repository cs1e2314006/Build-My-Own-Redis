package Main; // Declares the package this class belongs to

import java.io.*; // Imports classes for input and output operations
import java.net.Socket; // Imports the Socket class for network communication
import java.nio.charset.StandardCharsets; // Imports the StandardCharsets class for character encoding
import java.util.concurrent.ConcurrentHashMap; // Imports the ConcurrentHashMap class for thread-safe data storage

public class ReplicaClient { // Defines the ReplicaClient class

    static ConcurrentHashMap<String, String> ReplicaStore; // Declares a static thread-safe map to store replica data
                                                           // (key-value pairs)
    private static ConcurrentHashMap<String, Long> ReplicaExpiry; // Declares a static thread-safe map to store key
                                                                  // expiration times

    public static void connectToMaster(String host, int port, ConcurrentHashMap<String, String> store, // Defines a
                                                                                                       // method to
                                                                                                       // connect to the
                                                                                                       // master server
            ConcurrentHashMap<String, Long> expiry) { // Parameters: host, port, store, expiry

        new Thread(() -> { // Creates a new thread to handle the connection to the master
            ReplicaStore = store; // Assigns the provided store to the ReplicaStore
            ReplicaExpiry = expiry; // Assigns the provided expiry map to the ReplicaExpiry
            SetGetHandler.startExpiryCleanup(ReplicaStore, ReplicaExpiry); // Starts a cleanup thread to remove expired
                                                                           // keys
            try (Socket socket = new Socket(host, port); // Creates a socket to connect to the master
                    BufferedReader reader = new BufferedReader( // Creates a buffered reader to read data from the
                                                                // master
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8)); // Uses UTF-8
                                                                                                     // encoding
                    OutputStream out = socket.getOutputStream()) { // Creates an output stream to send data to the
                                                                   // master

                boolean pingSuccess = false; // Flag to track if the PING command was successful
                boolean replconf1Success = false; // Flag to track if the first REPLCONF command was successful
                boolean replconf2Success = false; // Flag to track if the second REPLCONF command was successful

                String PingCommand = "*1\r\n$4\r\nPING\r\n"; // Defines the PING command in RESP format
                String response = sendCommand(PingCommand, out, reader); // Sends the PING command and receives the
                                                                         // response
                System.out.println("Ping Response: " + response); // Prints the PING response
                if (response != null && response.equals("+PONG")) { // Checks if the response is "+PONG"
                    pingSuccess = true; // Sets the pingSuccess flag to true
                }

                String Replcommand = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"; // Defines the
                                                                                                        // REPLCONF
                                                                                                        // listening-port
                                                                                                        // command
                response = sendCommand(Replcommand, out, reader); // Sends the REPLCONF command and receives the
                                                                  // response
                System.out.println("Replconf Response: " + response); // Prints the REPLCONF response
                if (response != null && response.equals("+OK")) { // Checks if the response is "+OK"
                    replconf1Success = true; // Sets the replconf1Success flag to true
                }

                String Replconf2ndcommand = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"; // Defines the
                                                                                                      // REPLCONF capa
                                                                                                      // command
                response = sendCommand(Replconf2ndcommand, out, reader); // Sends the REPLCONF command and receives the
                                                                         // response
                System.out.println("Replconf2nd Response: " + response); // Prints the REPLCONF response
                if (response != null && response.equals("+OK")) { // Checks if the response is "+OK"
                    replconf2Success = true; // Sets the replconf2Success flag to true
                }

                if (pingSuccess && replconf1Success && replconf2Success) { // Checks if all handshake commands were
                                                                           // successful
                    String PsyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$1\r\n-1\r\n"; // Defines the PSYNC command
                    response = sendCommand(PsyncCommand, out, reader); // Sends the PSYNC command and receives the
                                                                       // response
                    System.out.println("Psync Response: " + response); // Prints the PSYNC response

                    // After handshake, start listening for commands
                    String line; // Declares a variable to store each line received from the master
                    while ((line = reader.readLine()) != null) { // Loops while there is data to read from the master
                        System.out.println("Received command from master: " + line); // Prints the command received from
                                                                                     // the master
                        String[] commandParts = parseRespCommand(line, reader); // Parses the RESP command into parts
                        if (commandParts != null && commandParts.length > 0) { // Checks if the command was parsed
                                                                               // successfully
                            String commandName = commandParts[0].toUpperCase(); // Extracts the command name and
                                                                                // converts it to uppercase
                            if (commandName.equals("SET")) { // Checks if the command is SET
                                applySetCommand(commandParts); // Applies the SET command to the replica store
                            }
                            // Handle other commands (DEL, etc.)
                        }
                    }
                } else { // If the handshake failed
                    System.err.println("Handshake failed. Aborting replication."); // Prints an error message
                }

            } catch (Exception e) { // Catches any exceptions that occur
                e.printStackTrace(); // Prints the stack trace of the exception
            }
        }).start(); // Starts the thread
    }

    private static String sendCommand(String command, OutputStream out, BufferedReader reader) throws IOException { // Defines
                                                                                                                    // a
                                                                                                                    // method
                                                                                                                    // to
                                                                                                                    // send
                                                                                                                    // a
                                                                                                                    // command
                                                                                                                    // to
                                                                                                                    // the
                                                                                                                    // master
        try {
            System.out.println("Sending: " + command.replace("\r\n", "\\r\\n")); // Prints the command being sent (for
                                                                                 // debugging)
            out.write(command.getBytes(StandardCharsets.UTF_8)); // Sends the command as bytes using UTF-8 encoding
            out.flush(); // Ensures all data is sent immediately
            return reader.readLine(); // Read only one line
        } catch (IOException e) { // Catches any IOExceptions that occur
            System.err.println("Error communicating with server: " + e.getMessage()); // Prints an error message
            return null;
        }
    }

    private static String[] parseRespCommand(String line, BufferedReader reader) throws IOException { // Defines a
                                                                                                      // method to parse
                                                                                                      // a RESP command
        if (line.startsWith("*")) { // Checks if the line starts with "*" (RESP array indicator)
            int numArgs = Integer.parseInt(line.substring(1)); // Extracts the number of arguments from the line
            String[] args = new String[numArgs]; // Creates an array to store the arguments
            for (int i = 0; i < numArgs; i++) { // Loops through each argument
                reader.readLine(); // Skip length line // Reads and discards the line containing the length of the
                                   // argument
                args[i] = reader.readLine(); // Reads the argument itself
            }
            return args; // Returns the array of arguments
        }
        return null; // Returns null if the line is not a RESP array
    }

    private static void applySetCommand(String[] commandParts) { // Defines a method to apply the SET command to the
                                                                 // replica store
        if (commandParts.length == 3) { // Checks if the command has the correct number of arguments
            String key = commandParts[1]; // Extracts the key from the command parts
            String value = commandParts[2]; // Extracts the value from the command parts
            ReplicaStore.put(key, value); // Puts the key-value pair into the replica store
            System.out.println("Replica: SET " + key + " " + value); // Prints a message indicating the SET command was
                                                                     // applied
        } else { // If the command has the wrong number of arguments
            System.err.println("Replica: Invalid SET command format"); // Prints an error message
        }
    }

}
