package rdb;

import rdb.authenticator.Authenticate;
import rdb.parser.Parser;
import rdb.syntaxchecker.Syntax;
import rdb.executor.Execute;

import java.util.Scanner;

/**
 * Main class for the database program
 */
public class Home {
    public static void main(String[] args) {
        // Login the user
        Authenticate auth = new Authenticate();

        // Loop to accept commands from the user
        while (true) {
            // Read command from console
            System.out.print("rdb>");
            Scanner scanner = new Scanner(System.in);
            StringBuilder line = new StringBuilder(scanner.nextLine());

            line = new StringBuilder(line.toString().replaceAll("\\s+$", ""));
            // Read from console until ';' is entered in the console
            while (!line.toString().endsWith(";")) {
                System.out.print("  ->");
                scanner = new Scanner(System.in);
                String newLine = scanner.nextLine();
                newLine = newLine.replaceAll(" +$", "");
                line.append(newLine);
            }

            // Split the commands based on ';'
            String command = line.toString().replaceAll(";$","");
            Parser parser = new Parser();
            String[] commands = parser.parseCommand(command);

            // Check for Syntax
            Syntax syntax = new Syntax();
            if(!syntax.check(commands))
                continue;

            // Execute commands
            Execute execute = new Execute();
            execute.executeCommand(commands);

            // Exit program condition
            if (line.toString().equalsIgnoreCase("exit;"))
                break;
        }
    }
}
