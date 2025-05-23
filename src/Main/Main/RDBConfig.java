package Main;

public class RDBConfig {
    // === RDB CONFIGURATION PARAMETERS (added for RDB support) ===
    private static String dir = "/tmp"; // default directory
    private static String dbfilename = "dump.rdb"; // default filename

    // Parses command-line arguments to set RDB configuration
    public static void parseArguments(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--dir") && i + 1 < args.length) {
                dir = args[i + 1];
            }
            if (args[i].equals("--dbfilename") && i + 1 < args.length) {
                dbfilename = args[i + 1];
            }
        }
    }

    // Getter for directory path
    public static String getDir() {
        return dir;
    }

    // Getter for RDB file name
    public static String getDbfilename() {
        return dbfilename;
    }
}
