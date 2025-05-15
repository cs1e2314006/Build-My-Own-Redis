package Main;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class SetGetHandler extends Thread {
    private final Socket client;
    private final String[] args;
    private final ConcurrentHashMap<String, String> store;

    public SetGetHandler(Socket client, String[] args, ConcurrentHashMap<String, String> store) {
        this.client = client;
        this.args = args;
        this.store = store;
    }

    @Override
    public void run() {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()))) {
            String command = args[0].toUpperCase();

            switch (command) {
                case "SET":
                    if (args.length != 3) {
                        writer.write("-ERR wrong number of arguments for 'SET'\r\n");
                    } else {
                        store.put(args[1], args[2]);
                        writer.write("+OK\r\n");
                    }
                    break;

                case "GET":
                    if (args.length != 2) {
                        writer.write("-ERR wrong number of arguments for 'GET'\r\n");
                    } else {
                        String value = store.get(args[1]);
                        if (value == null) {
                            writer.write("$-1\r\n");
                        } else {
                            writer.write("$" + value.length() + "\r\n" + value + "\r\n");
                        }
                    }
                    break;

                default:
                    writer.write("-ERR unknown command\r\n");
            }

            writer.flush();
            client.close();
        } catch (IOException e) {
            System.out.println("Handler error: " + e.getMessage());
        }
    }
}
