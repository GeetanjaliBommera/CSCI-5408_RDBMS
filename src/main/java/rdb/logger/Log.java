package rdb.logger;

import rdb.utilities.Files;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class to generate log files
 */
public class Log {
    private static final Files genLogFile = new Files();
    private static final Files eventLogFile = new Files();
    private static final Files queryLogFile = new Files();
    private static final Date date = new Date();

    /*
     Creates and writes log information to the General Log
     */
    public static void generalLog(String information) {
        File file = genLogFile.createFile("Logs/GenLogs", "GenLog_" + new SimpleDateFormat("yyyy_MM_dd").format(date) + ".log");
        genLogFile.writeFile(file, information);
    }

    /*
     Creates and writes log information to the Event Log
     */
    public static void eventLog(String information) {
        File file = eventLogFile.createFile("Logs/EventLogs", "EventLog_" + new SimpleDateFormat("yyyy_MM_dd").format(date) + ".log");
        eventLogFile.writeFile(file, information);
    }

    /*
     Creates and writes log information to the Query Log
     */
    public static void queryLog(String information) {
        File file = queryLogFile.createFile("Logs/QueryLogs", "QueryLog_" + new SimpleDateFormat("yyyy_MM_dd").format(date) + ".log");
        queryLogFile.writeFile(file, information);
    }
}
