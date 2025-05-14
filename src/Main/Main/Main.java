package Main;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        // Print a log that the server has started
        System.out.println("Server started at port 6379");

        int port = 6379; // Port number used by Redis server

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true); // Allow socket to be reused quickly after close

            // Continuously accept new client connections
            while (true) {
                Socket client = serverSocket.accept(); // Accept a new client
                System.out.println("Client connected.");

                // Create a reader to read the first command from client
                BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                String firstLine = reader.readLine(); // Read first line of input

                // If client sends nothing, skip to next connection
                if (firstLine == null) {
                    client.close();
                    continue;
                }

                // Check if the command follows Redis RESP format
                if (firstLine.startsWith("*")) {
                    int argsCount = Integer.parseInt(firstLine.substring(1)); // Number of arguments
                    String[] arguments = new String[argsCount]; // To store the command and its arguments

                    // Loop to read arguments line-by-line
                    for (int i = 0; i < argsCount; i++) {
                        reader.readLine(); // Skip the length line (like $4)
                        arguments[i] = reader.readLine(); // Actual argument
                    }

                    String command = arguments[0].toUpperCase(); // Convert to uppercase for comparison
                    System.out.println("Command: " + command); // Debug log

                    // Choose thread based on command
                    switch (command) {
                        case "PING":
                            MultipleResponses pingThread = new MultipleResponses(client, arguments);
                            pingThread.start(); // Start PING handler thread
                            break;

                        case "ECHO":
                            Echo echoThread = new Echo(client, arguments);
                            echoThread.start(); // Start ECHO handler thread
                            break;

                        default:
                            // If command is unknown, send error
                            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
                            writer.write("-ERR unknown command\r\n");
                            writer.flush();
                            client.close(); // Close connection
                    }
                } else {
                    // If command doesn't follow RESP format
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
                    writer.write("-ERR invalid protocol\r\n");
                    writer.flush();
                    client.close(); // Close invalid connection
                }
            }

        } catch (IOException e) {
            // Log server-side errors
            System.out.println("Server Error: " + e.getMessage());
        }
    }
}
