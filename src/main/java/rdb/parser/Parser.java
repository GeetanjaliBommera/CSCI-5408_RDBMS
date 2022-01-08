package rdb.parser;

import rdb.logger.Log;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class to generate multiple commands from the inputs received from the console
 */
public class Parser {

    /*
     Generates multiple commands from the single input if there are many
     */
    public String[] parseCommand(String input) {
        Date date = new Date();
        // Split the input based on ';'
        String[] commands = input.split(";");
        for (int i = 0; i < commands.length; i++) {
            commands[i] = removeSpaces(commands[i]);
            //System.out.println(commands[i]);
            Log.queryLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS").format(date) + ", " + commands[i]);
        }
        return commands;
    }

    /*
     Removes whitespaces from the input string
     */
    public String removeSpaces(String command) {
        String trimCommand = command.replaceAll("\\s+", " ");
        trimCommand = trimCommand.replaceAll("^ ", "");
        trimCommand = trimCommand.replaceAll(" $", "");
        return trimCommand;
    }
}
