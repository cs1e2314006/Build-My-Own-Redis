package Main;

import java.io.BufferedWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.TreeMap;
import java.util.Objects; // For Objects.requireNonNull

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
}