package rdb.syntaxchecker;

/**
 * Class that performs syntax check of the input command
 */
public class Syntax {

    /*
     Checks the syntax of use command
     */
    public boolean use(String[] keys) {
        if (keys.length != 2)
            return false;

        return keys[1].matches("[a-zA-Z0-9]+");
    }

    /*
     Checks the syntax of create command
     */
    public boolean create(String[] keys) {
        boolean status;

        if (keys.length < 3) {
            return false;
        }

        switch (keys[1].toLowerCase()) {
            case "table": {
                if (keys.length != 4)
                    return false;
                status = keys[2].matches("[a-zA-Z0-9]+");
                if (!status)
                    return false;

                String[] columns = keys[3].split(",");
                for (String column : columns) {
                    column = column.replaceAll("^:+", "");
                    column = column.replaceAll(":+$", "");
                    String[] data = column.split(":");

                    if (data.length < 2)
                        return false;

                    status = data[0].matches("[a-zA-Z0-9]+");
                    if (!status)
                        return false;

                    switch (data[1].toLowerCase()) {
                        case "int":
                        case "float":
                        case "varchar":
                            break;
                        default:
                            return false;
                    }

                    boolean key = false;
                    for (int i = 2; i < data.length; i++) {
                        switch (data[i].toLowerCase()) {
                            case "primary_key":
                            case "foreign_key":
                            case "not_null":
                            case "unique":
                                break;
                            default:
                                return false;
                        }

                        if (data[i].equalsIgnoreCase("primary_key") || data[i].equalsIgnoreCase("foreign_key")) {
                            if (key)
                                return false;
                            else
                                key = true;
                        }

                        if (data[i].equalsIgnoreCase("foreign_key")) {
                            try {
                                i++;
                                if (data[i].equalsIgnoreCase("references")) {
                                    i++;
                                    status = data[i].matches("[a-zA-Z0-9]+");
                                    if (!status)
                                        return false;
                                    i++;
                                    status = data[i].matches("[a-zA-Z0-9]+");
                                    if (!status)
                                        return false;
                                }
                            } catch (ArrayIndexOutOfBoundsException ex) {
                                return false;
                            }
                        }
                    }
                }
            }
            break;
            case "erd":
            case "dump":
            case "database": {
                if (keys.length != 3) {
                    status = false;
                } else {
                    status = keys[2].matches("[a-zA-Z0-9]+");
                }
            }
            break;
            default:
                status = false;
        }
        return status;
    }

    /*
     Checks the syntax of drop command
     */
    public boolean drop(String[] keys) {
        if (keys.length < 3) {
            return false;
        }

        boolean status;
        switch (keys[1].toLowerCase()) {
            case "database":
            case "table": {
                if (keys.length != 3) {
                    status = false;
                } else {
                    status = keys[2].matches("[a-zA-Z0-9]+");
                }
            }
            break;
            default:
                status = false;
        }
        return status;
    }

    /*
     Checks the syntax of delete command
     */
    public boolean delete(String[] keys) {
        boolean status;
        if (keys.length < 3) {
            return false;
        }
        if (!keys[1].equalsIgnoreCase("from")) {
            return false;
        }

        status = keys[2].matches("[a-zA-Z0-9]+");
        if (!status)
            return false;

        if (keys.length > 3) {
            try {
                if (!keys[3].equalsIgnoreCase("where")) {
                    return false;
                }
                StringBuilder condition = new StringBuilder();
                for (int i = 4; i < keys.length; i++) {
                    condition.append(keys[i]);
                }
                condition = new StringBuilder(condition.toString().replaceAll("\\s+", ""));

                return checkWhereClause(condition.toString());
            } catch (ArrayIndexOutOfBoundsException ex) {
                return false;
            }
        }
        return true;
    }

    /*
     Syntax check of condition in the where clause
     */
    public boolean checkWhereClause(String condition) {
        boolean status;
        boolean operation = false;
        if (condition.contains(">")) {
            operation = true;

            String[] operands = condition.split(">");
            if (operands.length != 2)
                return false;

            status = operands[0].matches("[a-zA-Z0-9]+");
            status &= operands[1].matches("[+-]?([0-9]*[.])?[0-9]+");
            if (!status)
                return false;
        }
        if (condition.contains("<")) {
            if (operation)
                return false;

            String[] operands = condition.split("<");
            if (operands.length != 2)
                return false;

            status = operands[0].matches("[a-zA-Z0-9]+");
            status &= operands[1].matches("[+-]?([0-9]*[.])?[0-9]+");
            if (!status)
                return false;

            operation = true;
        }
        if (condition.contains("=")) {
            if (operation)
                return false;

            String[] operands = condition.split("=");
            if (operands.length != 2)
                return false;

            status = operands[0].matches("[a-zA-Z0-9]+");
            if (operands[1].startsWith("'") || operands[1].startsWith("\"")) {
                status &= operands[1].matches("^['\"][a-zA-Z0-9]+['\"]$");
            } else {
                status &= operands[1].matches("[+-]?([0-9]*[.])?[0-9]+");
            }

            return status;
        }
        return true;
    }

    /*
     Checks the syntax of start command
     */
    public boolean start(String[] keys) {
        if (keys.length == 2) {
            return keys[1].equalsIgnoreCase("transaction");
        } else {
            return false;
        }
    }

    /*
     Checks the syntax of commit command
     */
    public boolean commit(String[] keys) {
        return keys.length == 1;
    }

    /*
     Checks the syntax of rollback command
     */
    public boolean rollback(String[] keys) {
        if (keys.length == 1) {
            return true;
        } else if (keys.length == 2 || keys.length > 3) {
            return false;
        } else {
            if (keys[1].equalsIgnoreCase("to")) {
                return keys[2].matches("[a-zA-Z0-9]+");
            } else {
                return false;
            }
        }
    }

    /*
     Checks the syntax of savepoint command
     */
    public boolean savepoint(String[] keys) {
        if (keys.length == 2) {
            return keys[1].matches("[a-zA-Z0-9]+");
        } else {
            return false;
        }
    }

    /*
     Checks the syntax of exit command
     */
    public boolean exit(String[] keys) {
        return keys.length == 1;
    }

    /*
     Checks the syntax of alter command
     */
    public boolean alter(String[] keys) {
        boolean status;

        try {
            if (!keys[1].equalsIgnoreCase("table"))
                return false;

            status = keys[2].matches("[a-zA-Z0-9]+");
            if (!status)
                return false;

            switch (keys[3].toLowerCase()) {
                case "add":
                    status = keys[4].matches("[a-zA-Z0-9]+");
                    if (!status)
                        return false;

                    switch (keys[5].toLowerCase()) {
                        case "int":
                        case "float":
                        case "varchar":
                            break;
                        default:
                            return false;
                    }

                    if (keys.length > 6)
                        return false;

                    break;
                case "drop":
                    if (!keys[4].equalsIgnoreCase("column"))
                        return false;

                    status = keys[5].matches("[a-zA-Z0-9]+");
                    if (!status)
                        return false;

                    if (keys.length > 6)
                        return false;

                    break;
                case "modify":
                    if (!keys[4].equalsIgnoreCase("column"))
                        return false;

                    status = keys[5].matches("[a-zA-Z0-9]+");
                    if (!status)
                        return false;

                    switch (keys[6].toLowerCase()) {
                        case "int":
                        case "float":
                        case "varchar":
                            break;
                        default:
                            return false;
                    }

                    if (keys.length > 7)
                        return false;
                    break;
                default:
                    return false;
            }
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
        return true;
    }

    /*
     Checks the syntax of select command
     */
    public boolean select(String[] keys) {
        boolean status;

        try {
            int index = 1;
            StringBuilder columns = new StringBuilder();
            while (!keys[index].equalsIgnoreCase("from")) {
                columns.append(keys[index++]);
            }
            columns = new StringBuilder(columns.toString().replaceAll("\\s+", ""));
            if (columns.toString().contains(",")) {
                String[] column = columns.toString().split(",");
                for (String col : column) {
                    status = col.matches("[a-zA-Z0-9]+");
                    if (!status)
                        return false;
                }
            } else {
                status = columns.toString().matches("[a-zA-Z0-9]+") || columns.toString().equals("*");
                if (!status)
                    return false;
            }

            StringBuilder tables = new StringBuilder();
            index++;
            while (index < keys.length && !keys[index].equalsIgnoreCase("where")) {
                tables.append(keys[index++]);
            }
            tables = new StringBuilder(tables.toString().replaceAll("\\s+", ""));
            if (tables.toString().contains(",")) {
                String[] table = tables.toString().split(",");
                for (String tab : table) {
                    status = tab.matches("[a-zA-Z0-9]+");
                    if (!status)
                        return false;
                }
            } else {
                status = tables.toString().matches("[a-zA-Z0-9]+");
                if (!status)
                    return false;
            }

            if (index != keys.length) {
                StringBuilder condition = new StringBuilder();
                for (int i = index + 1; i < keys.length; i++) {
                    condition.append(keys[i]);
                }
                condition = new StringBuilder(condition.toString().replaceAll("\\s+", ""));

                return checkWhereClause(condition.toString());
            }
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
        return true;
    }

    /*
     Checks the syntax of insert command
     */
    public boolean insert(String[] keys) {
        boolean status;

        try {
            if (!keys[1].equalsIgnoreCase("into"))
                return false;

            status = keys[2].matches("[a-zA-Z0-9]+");
            if (!status)
                return false;

            if (!keys[3].equalsIgnoreCase("values"))
                return false;

            if (keys[4].contains(",")) {
                String[] values = keys[4].split(",");
                for (String value : values) {
                    value = value.replaceAll("^:+", "");
                    value = value.replaceAll(":+$", "");

                    if (!(value.startsWith("'") || value.startsWith("\""))) {
                        status = value.matches("[+-]?([0-9]*[.])?[0-9]+");
                        if (!status)
                            return false;
                    }
                }
            } else {
                if (!(keys[4].startsWith("'") || keys[4].startsWith("\""))) {
                    keys[4] = keys[4].replaceAll("^:+", "");
                    keys[4] = keys[4].replaceAll(":+$", "");

                    status = keys[4].matches("[+-]?([0-9]*[.])?[0-9]+");
                    if (!status)
                        return false;
                }
            }
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
        return true;
    }

    /*
     Checks the syntax of update command
     */
    public boolean update(String[] keys) {
        boolean status;

        try {
            status = keys[1].matches("[a-zA-Z0-9]+");
            if (!status)
                return false;

            if (!keys[2].equalsIgnoreCase("set"))
                return false;

            int index = 3;
            StringBuilder columnValues = new StringBuilder();
            while (index < keys.length && !keys[index].equalsIgnoreCase("where")) {
                columnValues.append(keys[index++]);
            }
            columnValues = new StringBuilder(columnValues.toString().replaceAll("\\s+", ""));
            if (columnValues.toString().contains(",")) {
                String[] columnValue = columnValues.toString().split(",");
                for (String col : columnValue) {
                    String[] operands = col.split("=");
                    status = operands[0].matches("[a-zA-Z0-9]+");
                    if (!status)
                        return false;

                    if (!(operands[1].startsWith("'") || operands[1].startsWith("\""))) {
                        status = operands[1].matches("[+-]?([0-9]*[.])?[0-9]+");
                        if (!status)
                            return false;
                    }
                }
            } else {
                String[] operands = columnValues.toString().split("=");
                status = operands[0].matches("[a-zA-Z0-9]+");
                if (!status)
                    return false;

                if (!(operands[1].startsWith("'") || operands[1].startsWith("\""))) {
                    status = operands[1].matches("[+-]?([0-9]*[.])?[0-9]+");
                    if (!status)
                        return false;
                }
            }

            if (index != keys.length) {
                StringBuilder condition = new StringBuilder();
                for (int i = index + 1; i < keys.length; i++) {
                    condition.append(keys[i]);
                }
                condition = new StringBuilder(condition.toString().replaceAll("\\s+", ""));

                return checkWhereClause(condition.toString());
            }
        } catch (ArrayIndexOutOfBoundsException ex) {
            return false;
        }
        return true;
    }

    /*
     Checks if the provided command is valid
     */
    public boolean keywords(String[] keys) {
        boolean status;
        switch (keys[0].toLowerCase()) {
            case "use":
                status = use(keys);
                break;
            case "create":
                status = create(keys);
                break;
            case "drop":
                status = drop(keys);
                break;
            case "alter":
                status = alter(keys);
                break;
            case "select":
                status = select(keys);
                break;
            case "insert":
                status = insert(keys);
                break;
            case "update":
                status = update(keys);
                break;
            case "delete":
                status = delete(keys);
                break;
            case "start":
                status = start(keys);
                break;
            case "commit":
                status = commit(keys);
                break;
            case "rollback":
                status = rollback(keys);
                break;
            case "savepoint":
                status = savepoint(keys);
                break;
            case "exit":
                status = exit(keys);
                break;
            default:
                status = false;
        }
        return status;
    }

    /*
     Generates keys/tokens from the input command
     */
    public boolean keywordCheck(String command) {
        // Extract data between the parentheses and format data
        if (command.contains("(") && command.contains(")")) {
            String inBrackets = command.substring(command.indexOf("(") + 1, command.indexOf(")"));
            inBrackets = inBrackets.replace(" ", ":");
            String outBrackets = command.substring(0, command.indexOf("("));
            outBrackets = outBrackets.replaceAll(" +$", "");
            command = outBrackets + " " + inBrackets;
        }

        // Generate tokens by extracting each word seperated by spaces
        String[] keys = command.split(" ");
        return keywords(keys);
    }

    /*
     Checks the individual commands in the available commands
     */
    public boolean check(String[] commands) {
        for (int i = 0; i < commands.length; i++) {
            if (!keywordCheck(commands[i])) {
                System.out.println("Syntax error at line " + (i + 1));
                return false;
            }
        }
        return true;
    }
}
