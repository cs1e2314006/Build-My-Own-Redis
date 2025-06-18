package Main;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class ReadHelper {

    // Assume this is initialized and populated elsewhere
    public static void read(String[] arguments,
            BufferedWriter writer,
            ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> streams,
            ConcurrentHashMap<String, String> lastStreamIds) {

        Thread reader = new Thread(() -> {
            try {
                int argsCount = arguments.length;
                if (argsCount < 4) {
                    writer.write("-ERR wrong number of arguments for 'XREAD' command\r\n");
                    writer.flush();
                    return;
                }

                boolean isBlocking = false;
                long blockMillis = 0;
                int streamStartIndex = 1;

                // Check if BLOCK is used
                if (arguments[1].equalsIgnoreCase("block")) {
                    isBlocking = true;
                    try {
                        blockMillis = Long.parseLong(arguments[2]);
                    } catch (NumberFormatException e) {
                        writer.write("-ERR invalid BLOCK duration\r\n");
                        writer.flush();
                        return;
                    }
                    streamStartIndex = 3; // Skip block + time
                }

                if (!arguments[streamStartIndex].equalsIgnoreCase("streams")) {
                    writer.write("-ERR syntax error: expected 'streams'\r\n");
                    writer.flush();
                    return;
                }

                // Parse keys and starting IDs
                int numStreams = (argsCount - (streamStartIndex + 1)) / 2;
                List<String> keys = new ArrayList<>();
                List<String> startings = new ArrayList<>();

                for (int i = 0; i < numStreams; i++) {
                    keys.add(arguments[streamStartIndex + 1 + i]);
                }

                for (int i = 0; i < numStreams; i++) {
                    startings.add(arguments[streamStartIndex + 1 + numStreams + i]);
                }
                String lastid = lastStreamIds.get(keys.get(0));
                // Blocking behavior
                if (isBlocking) {
                    long deadline = System.currentTimeMillis() + blockMillis;

                    while (System.currentTimeMillis() < deadline) {
                        ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> updatedStreams = ClientHandler
                                .getStream();
                        String result = buildXReadResp(keys, startings, updatedStreams);

                        if (!result.equals("*0\r\n") && ClientHandler.getlastStream().get(keys.get(0)) != lastid) {

                            writer.write(result);
                            writer.flush();
                            return;
                        }

                        try {
                            Thread.sleep(5); // small polling interval
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt(); // good practice
                            break;
                        }
                    }

                    // Timeout: return RESP null bulk string
                    writer.write("$-1\r\n");
                    writer.flush();
                    return;
                }

                // Non-blocking behavior
                String result = buildXReadResp(keys, startings, streams);
                writer.write(result);
                writer.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        reader.start();
    }

    public static String buildXReadResp(List<String> streamKeys, List<String> lastIds,
            ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> streams) {
        StringBuilder resp = new StringBuilder();
        ArrayList<String> topLevelResp = new ArrayList<>();

        for (int i = 0; i < streamKeys.size(); i++) {
            String streamKey = streamKeys.get(i);
            String lastId = lastIds.get(i);

            TreeMap<String, ConcurrentHashMap<String, String>> entries = streams.get(streamKey);
            if (entries == null)
                continue;

            // Get all entries with IDs > lastId
            SortedMap<String, ConcurrentHashMap<String, String>> filtered = entries.tailMap(lastId, false);
            if (filtered.isEmpty())
                continue;

            List<String> entryList = new ArrayList<>();

            for (Map.Entry<String, ConcurrentHashMap<String, String>> entry : filtered.entrySet()) {
                String entryId = entry.getKey();
                ConcurrentHashMap<String, String> fields = entry.getValue();

                List<String> fieldValueResp = new ArrayList<>();
                for (Map.Entry<String, String> fv : fields.entrySet()) {
                    fieldValueResp.add(respBulkString(fv.getKey()));
                    fieldValueResp.add(respBulkString(fv.getValue()));
                }

                // Add entry ID and its field-value array
                entryList.add(respArray(new String[] {
                        respBulkString(entryId),
                        respArray(fieldValueResp.toArray(new String[0]))
                }));
            }

            // Stream name + all matching entries
            topLevelResp.add(respArray(new String[] {
                    respBulkString(streamKey),
                    respArray(entryList.toArray(new String[0]))
            }));
        }

        // Top level array: one per stream
        resp.append(respArray(topLevelResp.toArray(new String[0])));
        return resp.toString();
    }

    private static String respBulkString(String str) {
        return "$" + str.length() + "\r\n" + str + "\r\n";
    }

    private static String respArray(String[] elements) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.length).append("\r\n");
        for (String elem : elements) {
            sb.append(elem);
        }
        return sb.toString();
    }
}
