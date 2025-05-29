package Main;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaClient {
    public static void connectToMaster(String host, int port, ConcurrentHashMap<String, String> store) {
        new Thread(() -> {
            try {

                String PingCommand = "*1\r\n$4\r\nPING\r\n";
                String response = sendCommand(PingCommand, host, port);
                System.out.println(response);
                boolean Replconf = false;
                boolean Replconf2nd = false;
                if (response.equals("+PONG") && !Replconf) {
                    String Replcommand = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
                    response = sendCommand(Replcommand, host, port);
                    Replconf = true;

                }
                if (response.equals("+OK") && Replconf && !Replconf2nd) {
                    String Replconf2ndcommand = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                    sendCommand(Replconf2ndcommand, host, port);
                    Replconf2nd = true;
                }
                if (response.equals("+OK") && Replconf && Replconf2nd) {
                    String PsyncCommand = "*3\r\n$\r\nPSYNC\r\n$1\r\n?\r\n$1\r\n-1\r\n";
                    sendCommand(PsyncCommand, host, port);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static String sendCommand(String command, String host, int port) throws IOException {
        // Use try-with-resources to ensure the socket and streams are closed
        // automatically.
        String response = "";
        try (Socket socket = new Socket(host, port);
                OutputStream out = socket.getOutputStream();
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

            System.out.println("Sending: " + command.replace("\r\n", "\\r\\n")); // Print command sent (for debugging)
            out.write(command.getBytes(StandardCharsets.UTF_8)); // Send the command as bytes
            out.flush(); // Ensure all data is sent immediately

            // Read and print the server's response line-by-line
            String line;
            while (true) {
                line = reader.readLine();
                if (line == null) {
                    System.out.println("[Server closed connection or no more data]");
                    break;
                }
                System.out.println("Received: " + line); // Print each line of the response
                if (!line.equals("+OK"))
                    response = reader.readLine();
                else
                    response = line;
                // Basic check to stop reading: if no more characters are ready
                // This is a simple way to detect end of a single response.
                if (!reader.ready()) {
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error communicating with server: " + e.getMessage());
        }
        return response;
    }
}
