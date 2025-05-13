package Main;

import java.io.BufferedReader; // Used to read text from the client
import java.io.BufferedWriter; // Used to send text to the client
import java.io.InputStreamReader; // Converts bytes from the client into characters
import java.io.OutputStreamWriter; // Converts characters to bytes to send to the client
import java.net.Socket; // Represents a connection to a client
// import java.net.ServerSocket;  // Listens for incoming connections from clients
// import java.io.IOException;    // Handles input/output errors

// This class handles communication with a single client
class MultipleResponses extends Thread {
    Socket clientSocket; // This variable will hold the connection to the client

    // Constructor: called when a new client connects
    MultipleResponses(Socket clientSocket) {
        this.clientSocket = clientSocket; // Save the client connection for use in this thread
    }

    // This method runs when the thread starts
    public void run() {
        // Try-with-resources: automatically closes resources when done
        try (
                // Set up a way to read messages from the client
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                // Set up a way to send messages back to the client
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));) {
            // Keep talking to the client until the connection is closed
            while (!clientSocket.isClosed()) {
                String line = reader.readLine(); // Read a line of text sent by the client
                if (line == null)
                    break; // If the client closed the connection, exit the loop

                System.out.println("line: " + line); // Show what the client sent (for debugging)

                // Check if the line starts with '*', which means it's a command in Redis
                // protocol
                if (line.startsWith("*")) {
                    // Get the number of arguments in the command (e.g., *2 for two arguments)
                    Integer lengthOfArguments = Integer.parseInt(line.substring(1));
                    System.out.println(line); // Print the command for debugging

                    // Create an array to hold all the arguments
                    String[] Arguments = new String[lengthOfArguments];
                    System.out.println("length of args: " + lengthOfArguments);

                    // Loop to read each argument from the client
                    for (int i = 0; i < lengthOfArguments; i++) {
                        reader.readLine(); // Skip the line that tells the length of the next argument
                        Arguments[i] = reader.readLine(); // Read the actual argument value
                        System.out.println(Arguments[i]); // Print the argument for debugging
                    }

                    // The first argument is the command, like "PING"
                    String Command = Arguments[0].toUpperCase(); // Make it uppercase to ignore case
                    System.out.println("command: " + Command);

                    // If the command is "PING"
                    if (Command.equals("PING")) {
                        // If there is only one argument, respond with "+PONG"
                        if (Arguments.length == 1) {
                            writer.write("+PONG\r\n"); // Send a simple string response
                        } else {
                            // If there is a second argument, echo it back as a bulk string
                            String responseValue = Arguments.length > 1 ? Arguments[1] : "";
                            writer.write("$" + responseValue.length() + "\r\n" + responseValue + "\r\n"); // Send length
                                                                                                          // and value
                        }
                        writer.flush(); // Make sure the response is actually sent to the client
                    } else {
                        // If the command is not recognized, send an error message
                        writer.write("-ERR unknown command\r\n");
                        writer.flush();
                    }
                }
            }
        } catch (Exception e) {
            // If something goes wrong (like a network error), just exit the thread
        }
    }
}
