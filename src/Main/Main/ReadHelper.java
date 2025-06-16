package Main;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class ReadHelper {

    // Assume this is initialized and populated elsewhere

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
