package Main;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class for handling Redis 'XREAD' command logic.
 * This class processes read requests for streams, including blocking and
 * non-blocking modes.
 */
public class ReadHelper {

    /**
     * Executes the Redis 'XREAD' command. This method runs in a separate thread
     * to handle potential blocking operations without blocking the main server
     * thread.
     *
     * @param arguments     An array of strings representing the command arguments
     *                      passed by the client.
     * @param writer        A BufferedWriter used to send the response back to the
     *                      client.
     * @param streams       A ConcurrentHashMap representing the Redis streams data
     *                      structure.
     *                      Outer key: stream name (String).
     *                      Inner TreeMap key: entry ID (String), ensuring entries
     *                      are sorted by ID.
     *                      Innermost ConcurrentHashMap: fields and values of a
     *                      stream entry (String to String).
     * @param lastStreamIds A ConcurrentHashMap storing the last read ID for each
     *                      stream,
     *                      used to handle '$' in XREAD commands for fetching the
     *                      latest entries.
     */
    public static void read(String[] arguments,
            BufferedWriter writer,
            ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> streams,
            ConcurrentHashMap<String, String> lastStreamIds) {

        // Create and start a new thread to handle the XREAD command.
        // This is crucial for blocking operations (BLOCK option) so that the main
        // server thread remains free to handle other client requests.
        Thread reader = new Thread(() -> {
            try {
                int argsCount = arguments.length;

                // Validate the minimum number of arguments for XREAD.
                // An XREAD command typically requires at least: XREAD [BLOCK timeout] STREAMS
                // key [key ...] ID [ID ...]
                if (argsCount < 4) {
                    writer.write("-ERR wrong number of arguments for 'XREAD' command\r\n");
                    writer.flush();
                    return; // Exit if arguments are insufficient
                }

                boolean isBlocking = false;
                long blockMillis = 0; // Default block duration
                int streamStartIndex = 1; // Default starting index for the 'STREAMS' keyword

                // Check for the 'BLOCK' option in the command arguments.
                if (arguments[1].equalsIgnoreCase("block")) {
                    isBlocking = true;
                    try {
                        // Parse the blocking duration in milliseconds.
                        blockMillis = Long.parseLong(arguments[2]);
                    } catch (NumberFormatException e) {
                        // Handle invalid block duration (not a valid number).
                        writer.write("-ERR invalid BLOCK duration\r\n");
                        writer.flush();
                        return;
                    }
                    streamStartIndex = 3; // Adjust the starting index for 'STREAMS' keyword
                }

                // Verify that 'STREAMS' keyword is present at the expected position.
                if (!arguments[streamStartIndex].equalsIgnoreCase("streams")) {
                    writer.write("-ERR syntax error: expected 'streams'\r\n");
                    writer.flush();
                    return;
                }

                // Calculate the number of streams based on the remaining arguments.
                // The structure is "STREAMS key1 key2 ... ID1 ID2 ..."
                int numStreams = (argsCount - (streamStartIndex + 1)) / 2;
                List<String> keys = new ArrayList<>(); // List to store stream keys
                List<String> startings = new ArrayList<>(); // List to store starting IDs for each stream

                // Populate the list of stream keys.
                // Keys are located immediately after the 'STREAMS' keyword.
                for (int i = 0; i < numStreams; i++) {
                    keys.add(arguments[streamStartIndex + 1 + i]);
                }

                // Populate the list of starting IDs for each stream.
                // IDs are located after all the stream keys.
                for (int i = 0; i < numStreams; i++) {
                    String rawId = arguments[streamStartIndex + 1 + numStreams + i];
                    // If the ID is '$', use the latest ID for that stream.
                    // This simulates the behavior of Redis's XREAD command to fetch new entries.
                    if (rawId.equals("$")) {
                        String latestId = lastStreamIds.getOrDefault(keys.get(i), "0-0");
                        startings.add(latestId);
                    } else {
                        startings.add(rawId);
                    }
                }

                // Get the last seen ID for the first stream (used for blocking condition
                // check).
                String lastid = lastStreamIds.getOrDefault(keys.get(0), "0-0");

                // Handle blocking behavior if 'BLOCK' option was specified.
                if (isBlocking) {
                    // Determine the deadline for blocking. If blockMillis is 0, block indefinitely.
                    long deadline = (blockMillis == 0) ? Long.MAX_VALUE : System.currentTimeMillis() + blockMillis;

                    // Loop until the deadline is reached or data is found.
                    while (System.currentTimeMillis() < deadline) {
                        // Get the latest state of streams. In a real Redis server, this would
                        // involve a more sophisticated mechanism for notifying new data.
                        // Here, it's simulated by re-fetching the stream data.
                        ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> updatedStreams = ClientHandler
                                .getStream();
                        String result = buildXReadResp(keys, startings, updatedStreams);

                        // If a non-empty result is found AND the stream's last ID has changed
                        // (meaning new data arrived), send the response and exit.
                        if (!result.equals("*0\r\n") && ClientHandler.getlastStream().get(keys.get(0)) != lastid) {
                            writer.write(result);
                            writer.flush();
                            return; // Data found, exit blocking loop
                        }

                        try {
                            // Introduce a small delay to prevent busy-waiting and reduce CPU usage.
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            // If the thread is interrupted, restore the interrupted status
                            // and break out of the loop.
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    // If the loop finishes without finding data (timeout),
                    // send a RESP null bulk string indicating no data found.
                    writer.write("$-1\r\n");
                    writer.flush();
                    return;
                }

                // Handle non-blocking behavior.
                // Directly build the response based on the current state of streams.
                String result = buildXReadResp(keys, startings, streams);
                writer.write(result);
                writer.flush();

            } catch (IOException e) {
                // Print stack trace for any IOException during writing to the client.
                e.printStackTrace();
            }
        });

        // Start the reader thread.
        reader.start();
    }

    /**
     * Builds the RESP (REdis Serialization Protocol) formatted response for the
     * 'XREAD' command.
     * This method iterates through the requested streams and their entries,
     * filtering based on the provided starting IDs.
     *
     * @param streamKeys A list of stream names to read from.
     * @param lastIds    A list of starting IDs for each corresponding stream.
     * @param streams    The main data structure holding all the stream data.
     * @return A String containing the RESP formatted response, which is an array of
     *         arrays.
     *         Each inner array represents a stream, containing the stream name and
     *         an array of entries.
     *         Each entry is an array containing the entry ID and an array of
     *         key-value pairs.
     */
    public static String buildXReadResp(List<String> streamKeys, List<String> lastIds,
            ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> streams) {
        StringBuilder resp = new StringBuilder();
        // This list will hold the RESP formatted string for each stream found.
        ArrayList<String> topLevelResp = new ArrayList<>();

        // Iterate through each requested stream key and its corresponding starting ID.
        for (int i = 0; i < streamKeys.size(); i++) {
            String streamKey = streamKeys.get(i);
            String lastId = lastIds.get(i);

            // Get the TreeMap of entries for the current stream.
            TreeMap<String, ConcurrentHashMap<String, String>> entries = streams.get(streamKey);
            if (entries == null) {
                continue; // If the stream does not exist, skip to the next.
            }

            // Use tailMap to get all entries with IDs strictly greater than 'lastId'.
            // The 'false' argument ensures that 'lastId' itself is not included.
            SortedMap<String, ConcurrentHashMap<String, String>> filtered = entries.tailMap(lastId, false);
            if (filtered.isEmpty()) {
                continue; // If no entries are found after filtering, skip to the next stream.
            }

            List<String> entryList = new ArrayList<>(); // List to hold RESP formatted entries for the current stream.

            // Iterate through the filtered entries for the current stream.
            for (Map.Entry<String, ConcurrentHashMap<String, String>> entry : filtered.entrySet()) {
                String entryId = entry.getKey();
                ConcurrentHashMap<String, String> fields = entry.getValue();

                List<String> fieldValueResp = new ArrayList<>(); // List to hold RESP formatted key-value pairs for an
                                                                 // entry.
                // Iterate through the fields and values of the current entry.
                for (Map.Entry<String, String> fv : fields.entrySet()) {
                    fieldValueResp.add(respBulkString(fv.getKey())); // Add RESP bulk string for the field key.
                    fieldValueResp.add(respBulkString(fv.getValue())); // Add RESP bulk string for the field value.
                }

                // Add the entry ID and its field-value array to the entry list.
                // An entry is represented as an array: [entry_id, [field1_key, field1_value,
                // ...]].
                entryList.add(respArray(new String[] {
                        respBulkString(entryId),
                        respArray(fieldValueResp.toArray(new String[0]))
                }));
            }

            // Add the current stream's name and its list of entries to the top-level
            // response.
            // A stream's data is represented as an array: [stream_name, [entry1, entry2,
            // ...]].
            topLevelResp.add(respArray(new String[] {
                    respBulkString(streamKey),
                    respArray(entryList.toArray(new String[0]))
            }));
        }

        // Append the final top-level RESP array to the StringBuilder.
        // This array contains an array for each stream that had matching entries.
        resp.append(respArray(topLevelResp.toArray(new String[0])));
        return resp.toString();
    }

    /**
     * Converts a given string into a RESP Bulk String format.
     * Example: "hello" becomes "$5\r\nhello\r\n"
     *
     * @param str The string to convert.
     * @return The RESP Bulk String representation.
     */
    private static String respBulkString(String str) {
        return "$" + str.length() + "\r\n" + str + "\r\n";
    }

    /**
     * Converts an array of RESP elements into a RESP Array format.
     * Example: new String[]{"$5\r\nhello\r\n", "$5\r\nworld\r\n"} becomes
     * "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
     *
     * @param elements An array of strings, where each string is already in a valid
     *                 RESP format (e.g., bulk string, another array).
     * @return The RESP Array representation.
     */
    private static String respArray(String[] elements) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.length).append("\r\n"); // Add array header (* followed by number of elements)
        for (String elem : elements) {
            sb.append(elem); // Append each element
        }
        return sb.toString();
    }
}