package Main;

import java.io.BufferedWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects; // For Objects.requireNonNull
import java.util.SortedMap;

public class StreamHandler {

    // Define a custom exception within the StreamHandler class for specific ID
    // errors
    private static class StreamIdValidationException extends Exception {
        public StreamIdValidationException(String message) {
            super(message);
        }
    }

    public static void handleStreamCommand(String[] arguments,
            ConcurrentHashMap<String, String> lastStreamIds,
            ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> streams,
            BufferedWriter clientWriter) throws Exception {

        // 1. Check for minimum arguments: XADD <key> <id> <field> <value>
        if (arguments.length < 4) {
            clientWriter.write("-ERR wrong number of arguments for 'XADD' command\r\n");
            clientWriter.flush();
            return;
        }

        String streamKey = arguments[1]; // The stream key
        String requestedId = arguments[2]; // The ID provided by the client (e.g., "1526919030474-0" or "*")
        String finalEntryId; // This will hold the ID that is actually used and returned
        System.out.println("requested id parts:- " + requestedId);
        try {
            // Call the private helper method to get the validated/generated ID
            finalEntryId = generateAndValidateStreamId(streamKey, requestedId, lastStreamIds);
        } catch (StreamIdValidationException e) {
            clientWriter.write("-ERR " + e.getMessage() + "\r\n");
            clientWriter.flush();
            return;
        } catch (NumberFormatException e) {
            // This catches internal errors if lastStreamIds were malformed (e.g.,
            // "abc-def")
            clientWriter.write("-ERR " + e.getMessage() + "\r\n"); // Or a more generic "Internal server error"
            clientWriter.flush();
            return;
        }

        // --- Collect Field-Value Pairs ---
        // XADD <key> <id> <field1> <value1> [<field2> <value2> ...]
        // The field-value pairs start from arguments[3].
        // The number of arguments after the ID must be even.
        if ((arguments.length - 3) % 2 != 0) {
            clientWriter.write("-ERR odd number of arguments for field-value pairs\r\n");
            clientWriter.flush();
            return;
        }

        ConcurrentHashMap<String, String> entryData = new ConcurrentHashMap<>();
        for (int i = 3; i < arguments.length; i += 2) {
            String field = arguments[i];
            String value = arguments[i + 1];
            entryData.put(field, value);
        }

        // --- Store the new stream entry ---
        // Get or create the TreeMap for this streamKey
        // The computeIfAbsent method ensures thread-safe creation if the stream doesn't
        // exist
        TreeMap<String, ConcurrentHashMap<String, String>> streamEntries = streams.computeIfAbsent(streamKey,
                k -> new TreeMap<>());

        // Add the new entry to the TreeMap. TreeMap handles sorting by ID.
        streamEntries.put(finalEntryId, entryData);

        // Update the last ID for this stream
        lastStreamIds.put(streamKey, finalEntryId);
        System.out.println(streams);
        // --- Send Response ---
        System.out.println("Added stream: " + streamKey + " with data: " + entryData + " and ID: " + finalEntryId);
        // For debugging, you can print the full stream content, but keep it conditional
        // for production
        // System.out.println("Current stream content for " + streamKey + ": " +
        // streamEntries);
        clientWriter.write("$" + finalEntryId.length() + "\r\n" + finalEntryId + "\r\n"); // Redis responds with Bulk
                                                                                          // String
        clientWriter.flush();
    }

    /**
     * Generates a new stream ID or validates a provided stream ID against the last
     * ID for the stream.
     * This is a private helper method for StreamHandler.
     *
     * @param streamKey     The key of the stream.
     * @param requestedId   The ID provided by the client (e.g., "*" or
     *                      "timestamp-sequence").
     * @param lastStreamIds A map storing the last added ID for each stream.
     * @return The validated or newly generated stream ID.
     * @throws StreamIdValidationException If the provided ID is invalid or smaller
     *                                     than the last ID.
     * @throws NumberFormatException       If an internal last ID is malformed
     *                                     (should not happen with proper state).
     */
    private static String generateAndValidateStreamId(String streamKey,
            String requestedId,
            ConcurrentHashMap<String, String> lastStreamIds)
            throws StreamIdValidationException {

        // Ensure streamKey and requestedId are not null, though typically handled by
        // command parsing
        Objects.requireNonNull(streamKey, "streamKey must not be null");
        Objects.requireNonNull(requestedId, "requestedId must not be null");
        Objects.requireNonNull(lastStreamIds, "lastStreamIds map must not be null");

        String lastIdForStream = lastStreamIds.getOrDefault(streamKey, "0-0");
        String[] lastIdParts = lastIdForStream.split("-");
        long lastMillisecondsTime;
        int lastSequenceNumber;

        try {
            lastMillisecondsTime = Long.parseLong(lastIdParts[0]);
            lastSequenceNumber = Integer.parseInt(lastIdParts[1]);
        } catch (NumberFormatException e) {
            // This indicates an internal error if lastStreamIds contains invalid data.
            // This should ideally be prevented by ensuring only valid IDs are stored in
            // lastStreamIds.
            throw new NumberFormatException(
                    "Internal server error: Malformed last ID for stream " + streamKey + ": " + lastIdForStream);
        }

        if (requestedId.equals("*")) {
            long currentMillis = System.currentTimeMillis();
            if (currentMillis == lastMillisecondsTime) {
                return currentMillis + "-" + (lastSequenceNumber + 1);
            } else if (currentMillis > lastMillisecondsTime) {
                return currentMillis + "-" + "0";
            } else {
                // System clock went backward or last ID was in the future.
                // Redis's XADD * guarantees ID > last ID.
                // We ensure this by incrementing the last timestamp if current is less,
                // or incrementing the sequence number if timestamps are equal.
                // For a strict emulation: if currentMillis < lastMillisecondsTime, use
                // lastMillisecondsTime + 1, sequence 0
                return (lastMillisecondsTime + 1) + "-" + "0";
            }
        } else { // Explicit ID provided
            String[] requestedIdParts = requestedId.split("-");
            if (requestedIdParts.length < 2) {
                throw new StreamIdValidationException("Invalid ID format. Must be <milliseconds>-<sequence>");
            }
            if (requestedIdParts[1].equals("*")) {
                return requestedIdParts[0] + "-" + (lastSequenceNumber + 1);
            }
            long newMillisecondsTime;
            int newSequenceNumber;
            try {
                newMillisecondsTime = Long.parseLong(requestedIdParts[0]);
                newSequenceNumber = Integer.parseInt(requestedIdParts[1]);
            } catch (NumberFormatException e) {
                throw new StreamIdValidationException("Invalid ID format: timestamp or sequence is not a number");
            }

            // Rule 1: ID must be greater than 0-0
            if (newMillisecondsTime == 0 && newSequenceNumber == 0) {
                throw new StreamIdValidationException("The ID specified in XADD must be greater than 0-0");
            }

            // Rule 2: ID must be strictly greater than the last entry's ID
            if (newMillisecondsTime < lastMillisecondsTime ||
                    (newMillisecondsTime == lastMillisecondsTime && newSequenceNumber <= lastSequenceNumber)) {
                throw new StreamIdValidationException(
                        "The ID specified in XADD is equal or smaller than the target stream top item");
            }
            return requestedId; // Validated, so use the requested ID
        }
    }

    /**
     * Handles the Redis 'XRANGE' command, which retrieves a range of entries from a
     * stream.
     * The command format is typically `XRANGE key start_id end_id`.
     *
     * @param arguments An array of strings representing the command and its
     *                  parameters.
     *                  Expected format: `["XRANGE", "stream_key", "start_id",
     *                  "end_id"]`.
     * @param writer    A BufferedWriter used to send the RESP (Redis Serialization
     *                  Protocol)
     *                  formatted response back to the client.
     * @throws Exception If an I/O error occurs during writing to the client.
     */
    public static void rangeCommandHandler(String[] arguments, BufferedWriter writer) throws Exception {
        int argsCount = arguments.length;

        // Validate the number of arguments. XRANGE requires at least 4 arguments:
        // "XRANGE", "key", "start_id", "end_id"
        if (argsCount < 4) {
            writer.write("-ERR wrong number of arguments for 'XRANGE' command\r\n");
            writer.flush();
            return;
        }

        // Extract the stream key, starting ID, and ending ID from the arguments.
        String key = arguments[1];
        String starting = arguments[2];
        String ending = arguments[3];

        // Retrieve the global streams data structure. This is assumed to be
        // a ConcurrentHashMap where:
        // - Key: Stream name (String)
        // - Value: A TreeMap of stream entries, sorted by their IDs (String to
        // ConcurrentHashMap)
        // - The inner ConcurrentHashMap stores the field-value pairs of a stream entry.
        ConcurrentHashMap<String, TreeMap<String, ConcurrentHashMap<String, String>>> streams = ClientHandler
                .getStream();

        // Get the TreeMap specifically for the requested stream key.
        TreeMap<String, ConcurrentHashMap<String, String>> idMap = streams.get(key);

        // If the stream does not exist, send an empty array as a response.
        if (idMap == null) {
            writer.write("*0\r\n"); // RESP array with 0 elements
            writer.flush();
            return;
        }

        // This list will store the processed stream entries before formatting into
        // RESP.
        // Each inner list will contain [id, field1_key, field1_value, field2_key,
        // field2_value, ...]
        List<List<String>> resultList = new ArrayList<>();
        SortedMap<String, ConcurrentHashMap<String, String>> subMap; // To store the filtered range of entries.

        // Determine the sub-map based on the 'starting' and 'ending' IDs.
        // Redis XRANGE command supports special IDs '-' (min ID) and '+' (max ID).
        if (starting.equals("-")) {
            // If starting is '-', get all entries from the first key up to and including
            // 'ending'.
            subMap = idMap.subMap(idMap.firstKey(), true, ending, true);
        } else if (starting.equals("+")) {
            // If starting is '+', this implies fetching from 'ending' to the very last
            // entry.
            // This behavior with '+' as a starting ID might need careful consideration
            // based on Redis's exact XRANGE specification.
            subMap = idMap.subMap(ending, true, idMap.lastKey(), true);
        } else {
            // If both 'starting' and 'ending' are specific IDs, get the sub-map
            // from 'starting' (inclusive) to 'ending' (inclusive).
            subMap = idMap.subMap(starting, true, ending, true);
        }

        // For debugging purposes, print the subMap to the console.
        System.out.println(subMap);

        // Iterate through the filtered entries in the subMap.
        Iterator<Map.Entry<String, ConcurrentHashMap<String, String>>> iterator = subMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<String, String>> currentEntry = iterator.next();
            String id = currentEntry.getKey(); // Get the ID of the current stream entry.

            // Create a temporary list to hold the ID and its field-value pairs for the
            // current entry.
            List<String> partiaList = new ArrayList<>();
            partiaList.add(id); // Add the entry ID as the first element.

            // Get the map of field-value pairs for the current stream entry.
            Map<String, String> partialMap = currentEntry.getValue();

            // Iterate over the field-value pairs and add them sequentially to the temporary
            // list.
            partialMap.forEach((currentkey, currentval) -> {
                partiaList.add(currentkey); // Add the field name.
                partiaList.add(currentval); // Add the field value.
            });

            // Add the complete entry (ID + all field-value pairs) to the main result list.
            resultList.add(partiaList);
        }

        // Convert the list of stream entries into a RESP formatted string, ready to be
        // sent to the client.
        String result = RangeHelper(resultList);

        // For debugging, print the final RESP result to the console with escaped
        // newlines for readability.
        System.out.println(result.replace("\r\n", "\\r\\n"));

        // Write the RESP formatted result to the client's output stream.
        writer.write(result);
        writer.flush();
    }

    /**
     * Helper method to format a list of stream entries into a Redis Serialization
     * Protocol (RESP) array.
     * The output format adheres to Redis's XRANGE response structure:
     * An outer RESP Array containing inner RESP Arrays, where each inner array
     * represents a stream entry.
     * Each stream entry array typically contains:
     * [entry_ID (as bulk string), [field1_name, field1_value, field2_name,
     * field2_value, ...] (as an array of bulk strings)]
     *
     * @param lists A list where each element is a list representing a single stream
     *              entry.
     *              The inner list contains [id, key1, value1, key2, value2, ...] as
     *              strings.
     * @return A string formatted according to the RESP protocol, representing the
     *         XRANGE response.
     */
    private static String RangeHelper(List<List<String>> lists) {
        // Start building the main RESP array. The header indicates the total number of
        // stream entries being returned.
        String mainString = "*" + lists.size() + "\r\n";

        // Iterate through each stream entry (which is represented as an inner
        // List<String>).
        for (int i = 0; i < lists.size(); i++) {
            List<String> partilList = lists.get(i); // Get the current stream entry data.

            // Each stream entry itself is an RESP array.
            // According to Redis's XRANGE, an entry is an array of two elements:
            // 1. The entry ID.
            // 2. An array containing the field-value pairs.
            // Therefore, the header for an individual entry should be "*2\r\n".
            // The current implementation uses partilList.size(), which is (1 +
            // num_field_value_pairs * 2).
            // This might be a deviation from standard Redis XRANGE response format where
            // entry is [ID, [fields...]].
            // If `partilList.size()` is intended to represent the total number of bulk
            // strings *within* the entry,
            // then this line needs careful review for strict Redis compatibility.
            String partialString = "*" + partilList.size() + "\r\n";

            // Extract the entry ID, which is always the first element in the `partilList`.
            String id = partilList.get(0);

            // Format the entry ID. In Redis XRANGE, the ID is typically returned as a bulk
            // string
            // directly within the entry's array, not wrapped in another array of size 1.
            // The `*1\r\n` here suggests the ID itself is treated as a single-element
            // array.
            // This might be a custom RESP serialization, or a misunderstanding of Redis's
            // specific XRANGE response structure.
            // A standard Redis XRANGE response for an entry like `[ID, [field1, value1,
            // field2, value2]]` would look like:
            // `*2\r\n$ID_LEN\r\nID\r\n*FIELD_COUNT\r\n$KEY1_LEN\r\nKEY1\r\n$VAL1_LEN\r\nVAL1\r\n...`
            partialString += "*1\r\n" + "$" + id.length() + "\r\n" + id + "\r\n";

            // Format the field-value pairs as a separate RESP array.
            // The number of elements in this array is `(partilList.size() - 1)` because the
            // first element is the ID.
            partialString += "*" + (partilList.size() - 1) + "\r\n";

            // Iterate from the second element onwards (index 1) to get field keys and
            // values.
            // Each key and value is formatted as a RESP bulk string.
            for (int j = 1; j < partilList.size(); j++) {
                partialString += "$" + partilList.get(j).length() + "\r\n" + partilList.get(j) + "\r\n";
            }
            // Append the fully formatted current stream entry to the main response string.
            mainString += partialString;
        }
        // Return the complete RESP formatted string representing all matching stream
        // entries.
        return mainString;
    }
}
