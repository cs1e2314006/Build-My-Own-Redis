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
            ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> streams) {
        int argsCount = arguments.length;
        Thread reader = new Thread(() -> {
            if (argsCount < 4) {
                try {
                    writer.write("-ERR wrong number of arguments for 'XREAD' command\r\n");
                    writer.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            } else {
                ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> localStreams = streams;
                System.out.println("local stream :-\n"+localStreams);
                if (arguments[1].equalsIgnoreCase("block")) {
                    System.out.println("inside block");
                    try {
                        Thread.sleep(Long.parseLong(arguments[2]));
                    } catch (NumberFormatException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    localStreams = ClientHandler.getStream();
                    System.out.println("local stream :-\n"+localStreams);
                    String[] args = new String[argsCount - 2];
                    int j = 0;
                    for (int i = 0; i < argsCount; i++) {
                        if (i == 2 || i == 1)
                            continue;
                        args[j++] = arguments[i];
                    }
                    j = 0;
                    for (String cmd : args) {
                        arguments[j++] = cmd;
                    }
                }
                List<String> keys = new ArrayList<>();
                List<String> startings = new ArrayList<>();
                if (argsCount == 4) {
                    keys.add(arguments[2]);
                    startings.add(arguments[3]);
                } else {
                    // XREAD streams stream_key other_stream_key 0-0 0-1
                    if ((argsCount - 2) % 2 != 0) {
                        try {
                            writer.write("-ERR wrong number of arguments for 'XRANGE' command\r\n");
                            writer.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    int i = 2, j = 0;
                    while (j < (argsCount - 2) / 2) {
                        keys.add(arguments[i++]);
                        j++;
                    }
                    j = 0;
                    while (j < (argsCount - 2) / 2) {
                        startings.add(arguments[i++]);
                        j++;
                    }
                }
                String result = buildXReadResp(keys, startings, localStreams);
                System.out.println(result.replace("\r\n", "\\r\\n"));
                try {
                    writer.write(result);
                    writer.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
