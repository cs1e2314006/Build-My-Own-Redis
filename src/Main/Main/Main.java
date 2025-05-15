package Main;

// Import necessary classes for networking and input/output operations
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class Main {

    // A shared key-value store that allows multiple threads to safely read and
    // write data at the same time.
    private static final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        // Display a message indicating that the server has started
        System.out.println("Server started at port 6379");

        // Define the port number where the server will listen for connections
        int port = 6379;

        // Start a try-with-resources block that automatically closes the server socket
        // when done
        try (ServerSocket serverSocket = new ServerSocket(port)) {
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
                            new SetGetHandler(client, arguments, store).start();
                            break;

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
