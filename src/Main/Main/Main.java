package Main;

import java.net.Socket;
import java.net.ServerSocket;
import java.io.*;

// This is the main class that starts the server
public class Main {
    public static void main(String[] args) {
        // Print a message to show the server is starting
        System.out.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null; // This will listen for new clients
        Socket clientSocket = null; // This will hold the connection to a client
        int port = 6379; // The port number to listen on (6379 is the default for Redis)

        try {
            serverSocket = new ServerSocket(port); // Start listening for connections
            serverSocket.setReuseAddress(true); // Allow the server to restart quickly on the same port

            // Keep running forever, accepting new clients
            while (true) {
                Socket client = serverSocket.accept(); // Wait for a client to connect
                System.out.println("Client connected: "); // Print a message when a client connects

                // Create a new thread to handle this client
                MultipleResponses multipleResponses = new MultipleResponses(client);
                multipleResponses.start(); // Start the thread, so it can talk to the client
            }
        } catch (IOException e) {
            // If there is an error starting the server, print the error message
            System.out.println("IOException: " + e.getMessage());
        } finally {
            // When the server stops, close the client connection if it was open
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
