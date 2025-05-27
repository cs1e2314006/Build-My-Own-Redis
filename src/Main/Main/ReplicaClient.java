package Main;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaClient {
    public static void connectToMaster(String host, int port, ConcurrentHashMap<String, String> store) {
        new Thread(() -> {
            try (Socket socket = new Socket(host, port);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {

                // Send a PING or initial sync request
                out.write("*1\r\n$4\r\nPING\r\n");
                out.flush();

                // Continuously read commands from master and apply them to the local store
                String line;
                while ((line = in.readLine()) != null) {
                    // Parse and apply master updates (simple demo — needs RESP parsing)
                    System.out.println("Received from master: " + line);
                    // You'll expand this later to parse actual commands like SET
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }
}
