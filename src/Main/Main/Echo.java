package Main;

import java.io.*;
import java.net.Socket;

public class Echo extends Thread {
    Socket clientSocket; // Client connection socket
    String[] arguments; // Arguments received from client

    // Constructor: assign socket and arguments
    Echo(Socket clientSocket, String[] arguments) {
        this.clientSocket = clientSocket;
        this.arguments = arguments;
    }

    // This method runs in a new thread
    public void run() {
        try (
                // Create writer to send response to client
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {
            // If ECHO command is missing argument, return error
            writer.write("Response For Echo Command\r\n");
            if (arguments.length < 2) {
                writer.write("-ERR missing argument for ECHO\r\n");
            } else {
                // Concatenate all arguments except the first (command itself)
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i < arguments.length; i++) {
                    sb.append(arguments[i]).append(" ");
                }
                String response = sb.toString().trim(); // Final ECHO string

                // Respond using RESP bulk string format
                writer.write("$" + response.length() + "\r\n" + response + "\r\n");
            }
            writer.flush(); // Send response

        } catch (IOException e) {
            // Log if error occurs during ECHO handling
            System.out.println("ECHO Error: " + e.getMessage());
        }
    }
}
