package Main;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

public class RDBKeyHandler {
    public static void loadRdbFile(String dir, String dbfilename, ConcurrentHashMap<String, String> store) {
        File file = new File(dir, dbfilename);
        if (!file.exists()) return; // No RDB file

        try (InputStream in = new FileInputStream(file)) {
            // 1. Skip header (9 bytes)
            in.skip(9);

            // 2. Skip metadata (0xFA sections)
            int b;
            while ((b = in.read()) == 0xFA) {
                readString(in); // metadata name
                readString(in); // metadata value
            }

            // 3. Database section (starts with 0xFE)
            if (b != 0xFE) return; // No DB section

            readSize(in); // DB index (ignore)

            // 4. Hash table sizes (starts with 0xFB)
            if (in.read() != 0xFB) return;
            readSize(in); // hash table size (ignore)
            readSize(in); // expires hash table size (ignore)

            // 5. Read key-value pairs
            while (true) {
                in.mark(1);
                int next = in.read();
                if (next == 0xFF) break; // End of file section
                if (next == 0xFC) { // Expiry in ms
                    in.skip(8); // skip 8 bytes
                    next = in.read(); // Value type
                } else if (next == 0xFD) { // Expiry in seconds
                    in.skip(4); // skip 4 bytes
                    next = in.read(); // Value type
                }
                if (next == 0x00) { // String type
                    String key = readString(in);
                    String value = readString(in);
                    store.put(key, value);
                } else {
                    // Only handle string types for this stage
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int readSize(InputStream in) throws IOException {
        int first = in.read();
        int type = (first & 0xC0) >> 6;
        if (type == 0) return first & 0x3F;
        if (type == 1) return ((first & 0x3F) << 8) | in.read();
        if (type == 2) {
            return (in.read() << 24) | (in.read() << 16) | (in.read() << 8) | in.read();
        }
        throw new IOException("Unsupported size encoding");
    }

    private static String readString(InputStream in) throws IOException {
        int first = in.read();
        int type = (first & 0xC0) >> 6;
        int len;
        if (type == 0) {
            len = first & 0x3F;
        } else if (type == 1) {
            len = ((first & 0x3F) << 8) | in.read();
        } else if (type == 2) {
            len = (in.read() << 24) | (in.read() << 16) | (in.read() << 8) | in.read();
        } else {
            throw new IOException("Unsupported string encoding");
        }
        byte[] buf = new byte[len];
        in.read(buf);
        return new String(buf, StandardCharsets.UTF_8);
    }
}
