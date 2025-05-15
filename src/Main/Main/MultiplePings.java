package Main;

import java.io.*;
import java.net.Socket;

public class MultiplePings extends Thread {
    Socket clientSocket; // Client connection socket
    String[] arguments; // Arguments received from client

    // Constructor: assign socket and arguments
    MultiplePings(Socket clientSocket, String[] arguments) {
        this.clientSocket = clientSocket;
        this.arguments = arguments;
    }

    // This method runs in a new thread
    public void run() {
        try (
                // Create writer to send response to client
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {
            // If only PING is given without message, respond with +PONG
            writer.write("Response For Ping Command\r\n");
            if (arguments.length == 1) {
                writer.write("+PONG\r\n");
            } else {
                // If additional arguments are present, concatenate them and send as bulk string
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i < arguments.length; i++) {
                    sb.append(arguments[i]).append(" ");
                }
                String response = sb.toString().trim(); // Remove trailing space

                // Send response with RESP bulk string format
                writer.write("$" + response.length() + "\r\n" + response + "\r\n");
            }
            writer.flush(); // Send data to client

        } catch (IOException e) {
            // Log if error occurs while writing response
            System.out.println("PING Error: " + e.getMessage());
        }
    }
}
