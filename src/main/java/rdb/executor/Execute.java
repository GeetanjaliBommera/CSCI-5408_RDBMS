package rdb.executor;

import rdb.logger.Log;

import java.nio.file.Files;
import java.io.*;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Transaction class to perform transaction
 */
class Transaction {
    static int count = 0;
    int transactionID;
    static Map<String, Integer> globalLock = new HashMap<>();     // Table name, and Transaction ID
    static String currentDatabase = "";
    public static Map<Integer, Map<String, String>> transactionLog = new HashMap<>();
    public static final String pathDatabase = "Databases";
    public static boolean transactionStart = false;
    public static final String FILE_DELIMETER = "@@@";

    public Transaction() {
        transactionID = ++count;
    }

    /*
     Method to execute the use command
     */
    public void use(String[] commandTokens) {
        File f1 = new File(pathDatabase, commandTokens[1]);
        if (!f1.exists()) {
            System.out.println("Database with this name does not exist!");
        } else {
            currentDatabase = commandTokens[1];
            System.out.println("Database changed.");
        }
    }

    /*
     Method to execute the create table command
     */
    public void createTable(String[] commandTokens) {
        String metaData = null;
        try {
            File file = new File("Databases/" + currentDatabase + "/" + commandTokens[2] + ".txt");
            if (!file.exists()) {
                file = new File("Databases/" + currentDatabase + "/" + (transactionStart ? commandTokens[2] + "_temp" : commandTokens[2]) + ".txt");
                if (file.exists()) {
                    System.err.println("Table already exists");
                    return;
                }
//                if (file.createNewFile()) {
//                    if (transactionStart)
//                        insertTransactionLog("create", commandTokens[2]);
//
//                    FileWriter myWriter = new FileWriter("Databases/" + currentDatabase + "/" +
//                            (transactionStart ? commandTokens[2] + "_temp" : commandTokens[2]) + ".txt");
                    String[] columns = commandTokens[3].split(",");
                    for (int i = 0; i < columns.length; i++) {
                        String[] eachColumn = columns[i].split(":");
                        if (metaData != null) {
                            metaData = metaData + "column: " + eachColumn[0] + "\n" + "datatype: " + eachColumn[1] + "\n";
                        } else {
                            metaData = "column: " + eachColumn[0] + "\n" + "datatype: " + eachColumn[1] + "\n";
                        }
                        if (eachColumn.length > 2 && eachColumn[2].equals("primary_key")) {
                            metaData = metaData + "constraint: " + eachColumn[2] + "\n";
                        }
                        if (eachColumn.length > 3 && eachColumn[2].equals("foreign_key")) {
                            File referencetable = new File("Databases/" + currentDatabase + "/" + eachColumn[4] + ".txt");
                            if (referencetable.exists()) {
                                file = new File("Databases/" + currentDatabase + "/" + eachColumn[4] + ".txt");
                                rdb.utilities.Files files = new rdb.utilities.Files();
                                String dataReferenceTable = files.readFile(file);
                                if (dataReferenceTable.contains("column: " + eachColumn[5] + "\n" + "datatype: " + eachColumn[1] + "\n" + "constraint: " + "primary_key")) {
                                    metaData = metaData + "constraint: " + eachColumn[2] + " " + eachColumn[4] + " " + eachColumn[5] + "\n";
                                } else {
                                    System.out.println("Referenced column or data type does not match");
                                    return;
                                }
                            } else {
                                System.out.println("Referenced table does not exist");
                                return;
                            }
                        }
                    }
                    file.createNewFile();
                    if (transactionStart)
                        insertTransactionLog("create", commandTokens[2]);

                    FileWriter myWriter = new FileWriter("Databases/" + currentDatabase + "/" +
                            (transactionStart ? commandTokens[2] + "_temp" : commandTokens[2]) + ".txt");
                    metaData = metaData + "@@@";
                    myWriter.write(metaData);
                    myWriter.close();
                    String dbMetaData = commandTokens[2] + "\n";
                    Files.write(Paths.get("Databases/" + currentDatabase + "/" + currentDatabase + "MetaData.meta"), dbMetaData.getBytes(), StandardOpenOption.APPEND);
                    System.out.println("Table created successfully");
                //}
            } else {
                System.out.println("Table already exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     Method to execute the create database command
     */
    public void createDatabase(String[] commandTokens) throws IOException {
        String databaseName = commandTokens[2];
        File f1 = new File(pathDatabase, databaseName);
        if (f1.exists()) {
            System.err.println("Error: Database with this name already exists!");
        } else {
            f1.mkdir();
            String filePath = "./" + pathDatabase + "/" + databaseName + "/" + databaseName + "MetaData.meta";
            File f2 = new File(filePath);
            f2.createNewFile();
        }
    }

    /*
     Method to execute the create erd command
     */
    public void createERD(String[] commandTokens) throws IOException {
        File f1 = new File(pathDatabase, commandTokens[2]);
        if (!f1.exists()) {
            System.out.println("Database with this name does not exist!");
        } else {
            rdb.utilities.Files files = new rdb.utilities.Files();
            Map<String, Map<String, String>> tables = new HashMap<>();

            File[] allFiles = files.getFileList(commandTokens[2], ".txt");
            if (allFiles.length == 0) {
                System.out.println("Database is empty");
                return;
            }

            for (File file : allFiles) {
                String fileName = file.getName();
                fileName = fileName.substring(0, fileName.length() - 4);
                boolean checkDelimeter = false;
                String rowDetails = "";
                FileInputStream fs = new FileInputStream(file);

                Map<String, String> structure = new HashMap<>();
                String row = "";
                String cols = "";
                Scanner sc = new Scanner(fs);
                while (sc.hasNextLine()) {
                    String fieldDetails = sc.nextLine();

                    if (FILE_DELIMETER.equals(fieldDetails)) {
                        checkDelimeter = true;
                    }

                    if (!checkDelimeter) {
                        String[] meta = fieldDetails.split(":");
                        switch (meta[0]) {
                            case "column":
                                row = meta[1].trim();
                                cols += row + ",";
                                structure.put("col" + row, row);
                                break;
                            case "datatype":
                                structure.put("data" + row, meta[1].trim());
                                break;
                            case "constraint":
                                String[] keys = meta[1].split(" ");
                                if (keys[1].equals("foreign_key")) {
                                    structure.put("key" + row + "refTable", keys[2]);
                                    structure.put("key" + row + "refId", keys[3]);
                                }
                                structure.put("key" + row, keys[1]);
                                break;
                        }
                    } else if (checkDelimeter && !FILE_DELIMETER.equals(fieldDetails)) {
                        rowDetails = rowDetails + fieldDetails + ";";
                    }
                }
                sc.close();
                structure.put("cols", cols.substring(0, cols.length() - 1));
                if (!rowDetails.equals("")) {
                    rowDetails = rowDetails.substring(0, rowDetails.length() - 1);

                    int index = 0;
                    for (String col : structure.get("cols").split(",")) {
                        if (structure.containsKey("key" + col)) {
                            if (structure.get("key" + col).equals("foreign_key"))
                                break;
                        }
                        index++;
                    }

                    if (index != structure.get("cols").split(",").length) {
                        Map<String, Integer> keys = new HashMap<>();
                        for (String tableRow : rowDetails.split(";")) {
                            String[] rowCols = tableRow.split(",");
                            keys.put(rowCols[index], 0);
                        }
                        structure.put("count", Integer.toString(keys.size()));
                    } else {
                        structure.put("count", "0");
                    }
                    structure.put("total", Integer.toString(rowDetails.split(";").length));
                } else {
                    structure.put("count", "0");
                    structure.put("total", "0");
                }
                tables.put(fileName, structure);
            }

            Map<String, Map<String, String>> temp = tables;
            for (Map.Entry<String, Map<String, String>> entry : tables.entrySet()) {
                for (String cols : entry.getValue().get("cols").split(",")) {
                    if (entry.getValue().containsKey("key" + cols + "refTable")) {
                        String refTable = entry.getValue().get("key" + cols + "refTable");
                        Map<String, String> struct = temp.get(refTable);
                        struct.put("key" + entry.getValue().get("key" + cols + "refId") + "refTable", entry.getKey());
                        temp.put(refTable, struct);
                    }
                }
            }

            File file = new File("./DumpFiles/" + "ERD_" + commandTokens[2] + ".erd");
            if (file.exists()) {
                file.delete();
            }

            files = new rdb.utilities.Files();
            for (
                    Map.Entry<String, Map<String, String>> entry : temp.entrySet()) {
                files.writeFile(file, entry.getKey());
                files.writeFile(file, "--------------------");

                for (String cols : entry.getValue().get("cols").split(",")) {
                    String line = "";
                    line += entry.getValue().get("col" + cols) + ": ";
                    line += entry.getValue().get("data" + cols) + " ";
                    if (entry.getValue().containsKey("key" + cols)) {
                        if (entry.getValue().get("key" + cols).equals("foreign_key")) {
                            line += entry.getValue().get("key" + cols) + " --> ";
                            line += entry.getValue().get("key" + cols + "refTable") + " ";
                            line += "[Cardinality: N]";
                        } else if (entry.getValue().get("key" + cols).equals("primary_key")) {
                            line += entry.getValue().get("key" + cols);
                            if (entry.getValue().containsKey("key" + cols + "refTable")) {
                                line += " <-- " + entry.getValue().get("key" + cols + "refTable") + " ";
                                int total = Integer.parseInt(entry.getValue().get("total"));
                                int count = Integer.parseInt(entry.getValue().get("count"));
                                line += "[Cardinality: 1, Participation: " + ((count < total) ? "Partial]" : "Total]");
                            }
                        }
                    }
                    files.writeFile(file, line);
                }
                files.writeFile(file, "--------------------");
            }
        }
    }

    /*
     Method to execute the drop table command
     */
    public void dropTable(String[] commandTokens) throws IOException {
        String tableName = commandTokens[2];
        String tableFile = "./Databases/" + currentDatabase + "/" + tableName + ".txt";
        File file = new File(tableFile);
        if (transactionStart) {
            if (file.exists()) {
                if (!isTableDropped(tableName)) {
                    insertTransactionLog("drop", commandTokens[2]);
                    Files.copy(new File("./Databases/" + currentDatabase + "/" + commandTokens[2] + ".txt").toPath(),
                            new File("./Databases/" + currentDatabase + "/" + commandTokens[2] + "_temp.txt").toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            }
            tableFile = "Databases/" + currentDatabase + "/" + commandTokens[2] + "_temp.txt";
            file = new File(tableFile);
        }

        if (file.delete()) {
            System.out.println("Table deleted " + tableName);
            //Update MetaData
            FileInputStream fis = new FileInputStream("./Databases/" + currentDatabase + "/" + currentDatabase + "MetaData.meta");
            Scanner scanner = new Scanner(fis);
            String newContent = "";
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.equals(tableName)) {

                } else {
                    newContent = newContent + line + "\n";
                }

            }
            scanner.close();

            FileWriter writer1 = new FileWriter("./Databases/" + currentDatabase + "/" + currentDatabase + "MetaData.meta");
            BufferedWriter buffer1 = new BufferedWriter(writer1);
            buffer1.write(newContent);
            buffer1.close();


        } else {
            System.err.println("ERROR: Could not delete table - Check if the database and table name is correct");
        }

    }

    /*
     Checks if the table has been dropped
     */
    public boolean isTableDropped(String tableName) {
        boolean dropped = false;
        for (Map.Entry<Integer, Map<String, String>> entry : transactionLog.entrySet()) {
            if (entry.getValue().get("command").equals("drop")) {
                if (entry.getValue().get("table").equals(tableName)) {
                    dropped = true;
                    break;
                }
            }
        }
        return dropped;
    }

    /*
     Method to execute the drop database command
     */
    public void dropDatabase(String[] commandTokens) {
        String directoryToBeDeleted = commandTokens[2];
        File filepath = new File(pathDatabase + "\\" + directoryToBeDeleted);
        deleteDirectory(filepath);

    }

    /*
     Method to delete a directory along with its contents
     */
    public void deleteDirectory(File filepath) {
        File[] allContents = filepath.listFiles();

        if (allContents != null) {
            for (File temp : allContents) {
                //recursive delete
                System.out.println("Deleting  " + temp);
                deleteDirectory(temp);
            }
        }

        if (filepath.delete()) {
        } else {
            System.err.println("Error: Database " + filepath + "does not exist");
        }
    }

    /*
     Method to execute the create dump command
     */
    public void createDump(String[] commandTokens) throws IOException {

        String databaseName = commandTokens[2];
        ArrayList<String> constraint = new ArrayList<String>();
        String filePath = "./Databases/" + databaseName;
        File f = new File(filePath);
        String[] listTables = f.list();

        if (listTables == null) {
            System.err.println("Error: Database does not exist");
        }

        if (listTables != null && listTables.length > 1) {

            String dumpFileName = "./DumpFiles/" + databaseName + "Dump.txt";

            File dumpFile = new File(dumpFileName);

            if (dumpFile.exists()) {
                dumpFile.delete();
            }

            dumpFile.createNewFile();

            for (String listTable : listTables) {
                if (!listTable.contains("MetaData")) {
                    ArrayList<String> column = new ArrayList<String>();
                    ArrayList<String> datatype = new ArrayList<String>();
                    boolean checkDelimeter = false;
                    String columnDetails = "";
                    String rowDetails = "";
                    String fullPath = filePath + "/" + listTable;
                    FileInputStream fis = new FileInputStream(fullPath);
                    Scanner sc = new Scanner(fis);

                    while (sc.hasNextLine()) {
                        String fieldDetails = sc.nextLine();

                        if (FILE_DELIMETER.equals(fieldDetails)) {
                            checkDelimeter = true;
                        }

                        if (checkDelimeter == false)
                            columnDetails = columnDetails + fieldDetails + " ";
                        else if (checkDelimeter == true && !FILE_DELIMETER.equals(fieldDetails))
                            rowDetails = rowDetails + fieldDetails + " ";
                    }
                    sc.close();

                    String[] colSplit = columnDetails.split("\\s+");

                    for (int i = 0; i < colSplit.length; i++) {

                        if ("column:".equals(colSplit[i])) {
                            column.add(colSplit[i + 1]);
                        } else if ("datatype:".equals(colSplit[i])) {
                            datatype.add(colSplit[i + 1]);
                        } else if ("constraint:".equals(colSplit[i])) {
                            constraint.add(colSplit[i + 1]);
                        }
                    }

                    String fields = "";
                    String herefields = "";
                    for (int i = 0; i < column.size(); i++) {
                        herefields = column.get(i) + " " + datatype.get(i) + ",";
                        fields = fields + herefields;
                    }

                    BufferedWriter writer = new BufferedWriter(new FileWriter(dumpFileName, true));

                    listTable = listTable.substring(0, listTable.lastIndexOf('.'));
                    fields = fields.substring(0, fields.length() - 1);
                    String create_table = "CREATE TABLE " + listTable.toUpperCase() + " (" + fields.toUpperCase()
                            + ");\n";
                    writer.write(create_table);


                    if (!rowDetails.isEmpty()) {
                        String[] rowSplit = rowDetails.split("\\s+");
                        for (int i = 0; i < rowSplit.length; i++) {
                            String rowsInserts = "INSERT INTO " + listTable + " VALUES (" + rowSplit[i] + ");\n";
                            writer.write(rowsInserts);
                        }
                    }
                    writer.write("COMMIT;\n");
                    writer.close();

                }

            }
        } else if (listTables != null && listTables.length == 1) {
            System.out.println("INFO: No tables exist in this database");
        }

    }


    /*
     Method to execute the alter command
     */
    public void alter(String[] commandTokens) {

    }

    /*
     Method to execute the select command
     */
    public void select(String[] commandTokens) throws IOException {
        String filename = "Databases/" + currentDatabase + "/" + commandTokens[3] + (transactionStart ? "_temp" : "") + ".txt";
        File file = new File(filename);
        if (!file.exists()) {
            if (transactionStart) {
                if (isTableDropped(commandTokens[3])) {
                    System.err.println("Table does not exist");
                    return;
                }

                file = new File("./Databases/" + currentDatabase + "/" + commandTokens[3] + ".txt");
                if (file.exists()) {
                    File tempFile = new File("./Databases/" + currentDatabase + "/" + commandTokens[3] + "_temp.txt");
                    insertTransactionLog("select", commandTokens[3]);
                    if (!tempFile.exists()) {
                        Files.copy(new File("./Databases/" + currentDatabase + "/" + commandTokens[3] + ".txt").toPath(),
                                tempFile.toPath());
                    }
                } else {
                    System.out.println("Table does not exist");
                    return;
                }
            } else {
                System.out.println("Table does not exist");
                return;
            }
        }
        int flag = 0;
        HashMap<String, ArrayList<String>> data = new HashMap<String, ArrayList<String>>();
        ArrayList<String> columns = new ArrayList<String>();
        try {
            Scanner scan = new Scanner(file);
            String content = scan.toString();
            int c = 0;
            while (scan.hasNext()) {
                String temp = scan.nextLine();
                if (temp.contains("@@@")) {
                    flag = 1;
                }
                if (temp.contains("column:") && flag == 0) {
                    String[] row = temp.split(":");
                    String colname = row[1].trim();
                    data.put(colname, null);
                    columns.add(colname);
                    c++;

                } else if (flag == 1) {
                    flag++;
                } else if (flag > 1) {
                    String[] columndata = temp.split(",");
                    for (int i = 0; i < columndata.length; i++) {
                        ArrayList<String> values = new ArrayList<String>();
                        if (data.get(columns.get(i)) != null) {
                            values.addAll(data.get(columns.get(i)));
                        }
                        values.add(columndata[i]);
                        data.put(columns.get(i), values);
                    }

                }
            }
            scan.close();
            if (data.get(columns.get(0)) == null) {
                System.out.println("No rows in the table");
                return;
            }
            ArrayList<Integer> index = new ArrayList<Integer>();
            if (commandTokens.length > 4) {
                if (commandTokens[4].equals("where")) {
                    if (commandTokens[5].contains("=")) {
                        String[] condition = commandTokens[5].split("=");
                        String value = condition[1];
                        ArrayList<String> values = new ArrayList<String>();
                        values.addAll(data.get(condition[0]));
                        if (values.contains(value)) {
                            index.add(values.indexOf(value));
                        }
                    } else if (commandTokens[5].contains("<")) {
                        String[] condition = commandTokens[5].split("<");
                        int value = Integer.valueOf(condition[1]);
                        ArrayList<Integer> values = new ArrayList<Integer>();
                        ArrayList<String> temp = new ArrayList<String>();
                        temp.addAll(data.get(condition[0]));
                        for (int i = 0; i < temp.size(); i++) {
                            values.add(Integer.valueOf(temp.get(i)));
                            if (values.get(i) < value) {
                                index.add(i);
                            }
                        }

                    } else if (commandTokens[5].contains(">")) {
                        String[] condition = commandTokens[5].split(">");
                        int value = Integer.valueOf(condition[1]);
                        ArrayList<Integer> values = new ArrayList<Integer>();
                        ArrayList<String> temp = new ArrayList<String>();
                        temp.addAll(data.get(condition[0]));
                        for (int i = 0; i < temp.size(); i++) {
                            values.add(Integer.valueOf(temp.get(i)));
                            if (values.get(i) > value) {
                                index.add(i);
                            }
                        }

                    }


                }
            }
            if (commandTokens[1].equals("*")) {
                int length = columns.size();
                System.out.println();
                for (int i = 0; i < length; i++) {
                    if (commandTokens.length == 6 && commandTokens[4].equals("where") && index.size() == 0) {
                        System.out.println("No rows exists");
                        return;
                    }
                    String key = columns.get(i);
                    System.out.print(key + "\t");
                }
                System.out.println();
                int numberofrows = data.get(columns.get(0)).size();
                for (int j = 0; j < numberofrows; j++) {
                    for (int i = 0; i < length; i++) {
                        if (index.size() > 0) {
                            if (index.contains(j)) {
                                System.out.print(data.get(columns.get(i)).get(j) + "\t");
                            }
                        } else {
                            System.out.print(data.get(columns.get(i)).get(j) + "\t");
                        }
                    }
                    System.out.println();
                }
            } else {
                String[] columnsToDisplay = commandTokens[1].split(",");
                int lengthtodisplay = columnsToDisplay.length;
                int length = columns.size();
                System.out.println();
                for (int i = 0; i < lengthtodisplay; i++) {
                    for (int j = 0; j < length; j++) {
                        if (columns.get(j).equals(columnsToDisplay[i])) {
                            if (commandTokens.length == 6 && commandTokens[4].equals("where") && index.size() == 0) {
                                System.out.println("No rows exists");
                                return;
                            }
                            String key = columns.get(j);
                            System.out.print(key + "\t");
                        }
                    }
                }
                System.out.println();
                int numberofrows = data.get(columns.get(0)).size();
                for (int j = 0; j < numberofrows; j++) {
                    for (int k = 0; k < lengthtodisplay; k++) {
                        for (int i = 0; i < length; i++) {

                            if (columnsToDisplay[k].equals(columns.get(i))) {
                                if (index.size() > 0) {
                                    if (index.contains(j)) {
                                        System.out.print(data.get(columns.get(i)).get(j) + "\t");
                                    }
                                } else {
                                    System.out.print(data.get(columns.get(i)).get(j) + "\t");
                                }
                            }
                        }
                    }
                    System.out.println();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /*
     Method to execute the insert command
     */
    public void insert(String[] commandTokens) throws IOException {
        String filename = "Databases/" + currentDatabase + "/" + commandTokens[2] + ".txt";
        File file = new File(filename);
        if (transactionStart) {
            if (isTableDropped(commandTokens[2])) {
                System.err.println("Table does not exist");
                return;
            }

            if (file.exists()) {
                File tempFile = new File("./Databases/" + currentDatabase + "/" + commandTokens[2] + "_temp.txt");
                insertTransactionLog("insert", commandTokens[2]);
                if (!tempFile.exists()) {
                    Files.copy(new File("./Databases/" + currentDatabase + "/" + commandTokens[2] + ".txt").toPath(),
                            tempFile.toPath());
                }
            }
            filename = "Databases/" + currentDatabase + "/" + commandTokens[2] + "_temp.txt";
            file = new File(filename);
        }

        String data;
        int primaryKeyFlag = 0;
        int count = 0;
        if (file.exists()) {
            String[] dataValues = commandTokens[4].split(",");
            data = "\n";
            for (int i = 0; i < dataValues.length; i++) {
                data = data + dataValues[i];
                if (i < dataValues.length - 1) {
                    data = data + ",";
                }

            }
            int pkindex = 0;
            try {
                Scanner s = new Scanner(file);
                while (s.hasNext()) {
                    String temp = s.nextLine();
                    if (temp.contains("column:")) {
                        count++;
                        if (primaryKeyFlag == 0) {
                            pkindex++;
                        }
                    }
                    if (temp.contains("constraint: primary_key")) {
                        primaryKeyFlag = 1;
                    }
                }
                s.close();

                ArrayList<String> primarykeys = new ArrayList<String>();
                if (count == dataValues.length) {
                    if (primaryKeyFlag == 1) {
                        String pkcontent = Files.readAllLines(Paths.get(filename)).toString();
                        int x = pkcontent.indexOf("@@@") + 4;
                        int y = pkcontent.length() - 1;
                        if (y - x > 1) {
                            String pkvalues = pkcontent.substring(x, y);
                            String[] pkdata = pkvalues.split(",");
                            int t = 0;
                            for (int j = 0; j < pkdata.length; j++) {
                                t = pkindex - 1;
                                if (j % count == t) {
                                    String temp = pkdata[t];
                                    t = t + count;
                                    primarykeys.add(temp.trim());
                                }
                            }

                        }
                        if (!primarykeys.contains(dataValues[pkindex - 1])) {
                            Files.write(Paths.get(filename), data.getBytes(), StandardOpenOption.APPEND);
                        } else {
                            System.out.println("Primary key constraint error");
                        }
                    } else {
                        Files.write(Paths.get(filename), data.getBytes(), StandardOpenOption.APPEND);
                    }


                } else {
                    System.out.println("Incorrect values to save..Please try again");
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        } else {
            System.out.println("Table does not exists");
            return;
        }
    }

    /*
     Method to execute the update command
     */
    public void update(String[] commandTokens) throws IOException {

    	String tableToBeUpdated = commandTokens[1];
    	String tableFile = "./Databases/" + currentDatabase + "/" + tableToBeUpdated + ".txt";
    	File file = new File(tableFile);
        if (transactionStart) {
            if (isTableDropped(commandTokens[1])) {
                System.err.println("Table does not exist");
                return;
            }

            if (file.exists()) {
                File tempFile = new File("./Databases/" + currentDatabase + "/" + commandTokens[1] + "_temp.txt");
                insertTransactionLog("update", commandTokens[1]);
                if (!tempFile.exists()) {
                    Files.copy(new File("./Databases/" + currentDatabase + "/" + commandTokens[1] + ".txt").toPath(),
                            tempFile.toPath());
                }
            }
            tableFile = "Databases/" + currentDatabase + "/" + commandTokens[1] + "_temp.txt";
            file = new File(tableFile);
        }

    	if (file.exists()) {

    		boolean checkDelimeter = false;
    		String columnDetails = "";
    		String rowDetails = "";

    		FileInputStream fis = new FileInputStream(tableFile);
    		Scanner sc = new Scanner(fis);

    		while (sc.hasNextLine()) {
    			String fieldDetails = sc.nextLine();

    			if (FILE_DELIMETER.equals(fieldDetails)) {
    				checkDelimeter = true;
    			}

    			if (checkDelimeter == false)
    				columnDetails = columnDetails + fieldDetails + "\n";
    			else if (checkDelimeter == true && !FILE_DELIMETER.equals(fieldDetails))
    				rowDetails = rowDetails + fieldDetails + "\n";
    		}
    		sc.close();

    		columnDetails = columnDetails.trim();
    		rowDetails = rowDetails.trim();

    		ArrayList<String> column = new ArrayList<String>();
    		String[] colSplit = columnDetails.split("\\s+");

    		for (int i = 0; i < colSplit.length; i++) {
    			if ("column:".equals(colSplit[i])) {
    				column.add(colSplit[i + 1]);
    			}
    		}

    		String setFieldString = commandTokens[3];
    		String fieldName = setFieldString.split("=")[0];
    		String fieldValue = setFieldString.split("=")[1];

    		int num = 199;

    		for (int i = 0; i < column.size(); i++) {
    			if (fieldName.equals(column.get(i))) {
    				num = i;
    			}

    		}

    		if (num != 199 && !rowDetails.isEmpty() && commandTokens.length == 4) {
    			Scanner scanner = new Scanner(rowDetails);
    			String newRecordDetails = "";

    			while (scanner.hasNextLine()) {
    				String line = scanner.nextLine();

    				String[] rowContent = line.split(",");

    				rowContent[num] = fieldValue;
    				newRecordDetails = newRecordDetails + "\n";

    				for (int i = 0; i < rowContent.length; i++) {
    					newRecordDetails = newRecordDetails + rowContent[i] + ",";
    				}

    				newRecordDetails = newRecordDetails.substring(0, newRecordDetails.lastIndexOf(","));

    			}

    			FileWriter writer = new FileWriter(tableFile);
    			BufferedWriter buffer = new BufferedWriter(writer);
    			buffer.write(columnDetails + "\n" + FILE_DELIMETER + "\n");
    			buffer.write(newRecordDetails.trim());
    			buffer.close();
    			scanner.close();
    		} else if (num != 199 && !rowDetails.isEmpty() && commandTokens.length == 6) {

    			String conditionString = commandTokens[5];

    			String conditionFieldName = conditionString.split("=")[0];
    			String conditionFieldValue = conditionString.split("=")[1];
    			int pos = 0;
    			
    			for (int i = 0; i < column.size(); i++) {
    				if (conditionFieldName.equals(column.get(i))) {
    					pos = i;
    				}
    			}

    			Scanner scanner = new Scanner(rowDetails);
    			String newRecordDetails = "";

    			while (scanner.hasNextLine()) {
    				String line = scanner.nextLine();
    				String[] rowContent = line.split(",");

    				if (conditionFieldValue.equals(rowContent[pos])) {
    					rowContent[num] = fieldValue;
    				}

    				newRecordDetails = newRecordDetails + "\n";

    				for (int i = 0; i < rowContent.length; i++) {
    					newRecordDetails = newRecordDetails + rowContent[i] + ",";
    				}
    				newRecordDetails = newRecordDetails.substring(0, newRecordDetails.lastIndexOf(","));

    			}
    			scanner.close();

    			FileWriter writer = new FileWriter(tableFile);
    			BufferedWriter buffer = new BufferedWriter(writer);
    			buffer.write(columnDetails + "\n" + FILE_DELIMETER + "\n");
    			buffer.write(newRecordDetails.trim());
    			buffer.close();

		} else if (num == 199) {
			System.err.println("ERROR: Column does not exist");
		} else if (rowDetails.isEmpty()) {
			System.out.println("INFO:  No records in the Table to update");
		}

	} else {
		System.err.println("Table does not exist");
	}

}

    /*
     Method to execute the delete command
     */
    public void delete(String[] commandTokens) throws IOException {

        String tableName = commandTokens[2];
        String tableFile = "./Databases/" + currentDatabase + "/" + tableName + ".txt";
        File file = new File(tableFile);
        if (transactionStart) {
            if (isTableDropped(commandTokens[2])) {
                System.err.println("Table does not exist");
                return;
            }

            if (file.exists()) {
                File tempFile = new File("./Databases/" + currentDatabase + "/" + commandTokens[2] + "_temp.txt");
                insertTransactionLog("delete", commandTokens[2]);
                if (!tempFile.exists()) {
                    Files.copy(new File("./Databases/" + currentDatabase + "/" + commandTokens[2] + ".txt").toPath(),
                            tempFile.toPath());
                }
            }
            tableFile = "Databases/" + currentDatabase + "/" + commandTokens[2] + "_temp.txt";
            file = new File(tableFile);
        }

        if (file.exists()) {

            boolean checkDelimeter = false;
            String columnDetails = "";
            String rowDetails = "";

            FileInputStream fis = new FileInputStream(tableFile);
            Scanner sc = new Scanner(fis);

            while (sc.hasNextLine()) {
                String fieldDetails = sc.nextLine();

                if (FILE_DELIMETER.equals(fieldDetails)) {
                    checkDelimeter = true;
                }

                if (checkDelimeter == false)
                    columnDetails = columnDetails + fieldDetails + "\n";
                else if (checkDelimeter == true && !FILE_DELIMETER.equals(fieldDetails))
                    rowDetails = rowDetails + fieldDetails + "\n";
            }
            sc.close();

            columnDetails = columnDetails.trim();
            rowDetails = rowDetails.trim();
            ArrayList<String> column = new ArrayList<String>();
            String[] colSplit = columnDetails.split("\\s+");

            for (int i = 0; i < colSplit.length; i++) {
                if ("column:".equals(colSplit[i])) {
                    column.add(colSplit[i + 1]);
                }
            }

            if (rowDetails == "") {
                System.out.println("INFO: No records in the database");
            } else {
                if (commandTokens.length == 3) {
                    FileWriter writer = new FileWriter(tableFile);
                    BufferedWriter buffer = new BufferedWriter(writer);
                    buffer.write(columnDetails + "\n" + FILE_DELIMETER);
                    buffer.close();
                } else {

                    String conditionString = commandTokens[4];

                    String fieldName = conditionString.split("=")[0];
                    String fieldValue = conditionString.split("=")[1];
                    int num = 0;

                    for (int i = 0; i < column.size(); i++) {
                        if (fieldName.equals(column.get(i))) {
                            num = i;
                        }
                    }

                    Scanner scanner = new Scanner(rowDetails);
                    String newRecordDetails = "";
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();

                        String[] rowContent = line.split(",");

                        if (fieldValue.equals(rowContent[num])) {

                        } else {
                            newRecordDetails = newRecordDetails + line + "\n";
                        }

                    }

                    FileWriter writer1 = new FileWriter(tableFile);
                    BufferedWriter buffer1 = new BufferedWriter(writer1);
                    buffer1.write(columnDetails + "\n" + FILE_DELIMETER + "\n");
                    buffer1.write(newRecordDetails.trim());
                    buffer1.close();
                    scanner.close();
                }
            }

        } else {
            System.err.println("Table does not exist");
        }
    }

    /*
     Method to add details of the transaction to the transaction log
     */
    public void insertTransactionLog(String command, String table) {
        Map<String, String> transaction = new HashMap<>();
        transaction.put("command", command);
        transaction.put("table", table);
        transactionLog.put(transactionID, transaction);
    }

    /*
     Method to execute the start transaction command
     */
    public void start() {
        if (currentDatabase.equals("")) {
            System.out.println("No database selected. Please select a database.");
            return;
        }

        transactionStart = true;
        insertTransactionLog("start", "");
        rdb.utilities.Files files = new rdb.utilities.Files();
        for (File file : files.getFileList(currentDatabase, "_temp.txt")) {
            file.delete();
        }
    }

    /*
     Method to execute the commit command
     */
    public void commit() {
        transactionStart = false;
        for (Map.Entry<Integer, Map<String, String>> entry : transactionLog.entrySet()) {
            try {
                if (entry.getValue().get("table") != "") {
                    if (!entry.getValue().get("command").equals("drop")) {
                        Files.copy(new File("./Databases/" + currentDatabase + "/" + entry.getValue().get("table") + "_temp.txt").toPath(),
                                new File("./Databases/" + currentDatabase + "/" + entry.getValue().get("table") + ".txt").toPath(), StandardCopyOption.REPLACE_EXISTING);

                        Files.delete(new File("./Databases/" + currentDatabase + "/" + entry.getValue().get("table") + "_temp.txt").toPath());
                    } else {
                        Files.delete(new File("./Databases/" + currentDatabase + "/" + entry.getValue().get("table") + ".txt").toPath());
                    }
                }
            } catch (Exception ex) {
                Date date = new Date();
                Log.eventLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS").format(date) +
                        ", Caught exception: " + ex.getMessage());
            }
        }
        transactionLog = new HashMap<>();
    }

    /*
     Method to execute the rollback command
     */
    public void rollback(String[] commandTokens) {
        transactionStart = false;
        for (Map.Entry<Integer, Map<String, String>> entry : transactionLog.entrySet()) {
            try {
                if (entry.getValue().get("table") != "") {
                    Files.delete(new File("./Databases/" + currentDatabase + "/" + entry.getValue().get("table") + "_temp.txt").toPath());
                }
            } catch (Exception ex) {
                Date date = new Date();
                Log.eventLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS").format(date) +
                        ", Caught exception: " + ex.getMessage());
            }
        }
        transactionLog = new HashMap<>();
    }

    /*
     Method to execute the savepoint command
     */
    public void savepoint(String[] commandTokens) {

    }
}

/**
 * Class that runs the transactions concurrently
 */
class TransactionRun extends Transaction implements Runnable {
    String[] commandTokens;

    public TransactionRun(String[] tokens) {
        super();
        commandTokens = tokens;
    }

    /*
     Method acquires locks for the specified tables by the current transaction
     */
    synchronized public void lockTables(String tables) throws InterruptedException {
        for (String table : tables.split(",")) {
            while (true) {
                if (globalLock.containsKey(table)) {
                    Date date = new Date();
                    Log.eventLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS").format(date) +
                            ", Transaction " + transactionID + ": table has been locked by " + globalLock.get(table));
                    Thread.sleep(100);
                } else {
                    globalLock.put(table, transactionID);
                    Date date = new Date();
                    Log.eventLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS").format(date) +
                            ", Transaction " + transactionID + " locked table " + table);
                    break;
                }
            }
        }
    }

    /*
    This method releases the lock obtained previously by the transaction
     */
    synchronized public void unlockTables(String tables) {
        for (String table : tables.split(",")) {
            globalLock.remove(table);
            Date date = new Date();
            Log.eventLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS").format(date) +
                    ", Transaction " + transactionID + " released locked table " + table);
        }
    }

    /*
     Run the transaction concurrently
     */
    public void run() {
        Date date = new Date();
        Log.eventLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS").format(date) +
                ", Transaction " + transactionID + " started");

        try {
            long startTime = System.nanoTime();

            switch (commandTokens[0].toLowerCase()) {
                case "use":
                    use(commandTokens);
                    break;
                case "create":
                    if (commandTokens[1].equalsIgnoreCase("database")) {
                        createDatabase(commandTokens);
                    } else if (commandTokens[1].equalsIgnoreCase("dump")) {
                        createDump(commandTokens);
                    } else if (commandTokens[1].equalsIgnoreCase("erd")) {
                        createERD(commandTokens);
                    } else {
                        StringBuilder columns = new StringBuilder();
                        for (String column : commandTokens[3].split(",")) {
                            column = column.replaceAll("^:+", "");
                            column = column.replaceAll(":+$", "");
                            columns.append(column).append(",");
                        }
                        commandTokens[3] = columns.substring(0, columns.toString().length() - 1);
                        if (commandTokens[1].equals("table") && currentDatabase != null && !currentDatabase.isEmpty()) {
                            lockTables(commandTokens[2]);
                            createTable(commandTokens);
                            unlockTables(commandTokens[2]);
                        } else {
                            System.out.println("No database is selected");
                        }
                    }
                    break;
                case "drop":
                    if (commandTokens[1].equalsIgnoreCase("database")) {
                        dropDatabase(commandTokens);
                    } else {
                        if (commandTokens[1].equals("table") && currentDatabase != null && !currentDatabase.isEmpty()) {
                            lockTables(commandTokens[2]);
                            dropTable(commandTokens);
                            unlockTables(commandTokens[2]);
                        } else {
                            System.out.println("No database is selected");
                        }
                    }
                    break;
                case "alter":
                    lockTables(commandTokens[2]);
                    alter(commandTokens);
                    unlockTables(commandTokens[2]);
                    break;
                case "select":
                    int index = 1;
                    StringBuilder columns = new StringBuilder();
                    while (!commandTokens[index].equalsIgnoreCase("from")) {
                        columns.append(commandTokens[index++]);
                    }
                    columns = new StringBuilder(columns.toString().replaceAll("\\s+", ""));

                    StringBuilder tables = new StringBuilder();
                    index++;
                    while (index < commandTokens.length && !commandTokens[index].equalsIgnoreCase("where")) {
                        tables.append(commandTokens[index++]);
                    }
                    tables = new StringBuilder(tables.toString().replaceAll("\\s+", ""));

                    String[] tokens = new String[4];
                    if (index != commandTokens.length) {
                        StringBuilder condition = new StringBuilder();
                        for (int i = index + 1; i < commandTokens.length; i++) {
                            condition.append(commandTokens[i]);
                        }
                        condition = new StringBuilder(condition.toString().replaceAll("\\s+", ""));

                        tokens = new String[6];
                        tokens[4] = "where";
                        tokens[5] = condition.toString();
                    }
                    tokens[0] = commandTokens[0];
                    tokens[1] = columns.toString();
                    tokens[2] = "from";
                    tokens[3] = tables.toString();

                    if (currentDatabase != null && !currentDatabase.isEmpty()) {
                        lockTables(tokens[3]);
                        select(tokens);
                        unlockTables(tokens[3]);
                    } else {
                        System.out.println("No database is selected");
                    }
                    break;
                case "insert":
                    columns = new StringBuilder();
                    for (String column : commandTokens[4].split(",")) {
                        column = column.replaceAll("^:+", "");
                        column = column.replaceAll(":+$", "");
                        column = column.replaceAll(":", " ");
                        columns.append(column).append(",");
                    }
                    commandTokens[4] = columns.substring(0, columns.toString().length() - 1);
                    if (currentDatabase != null && !currentDatabase.isEmpty()) {
                        lockTables(commandTokens[2]);
                        insert(commandTokens);
                        unlockTables(commandTokens[2]);
                    } else {
                        System.out.println("No database is selected");
                    }
                    break;
                case "update":
                    index = 3;
                    tokens = new String[4];
                    StringBuilder columnValues = new StringBuilder();
                    while (index < commandTokens.length && !commandTokens[index].equalsIgnoreCase("where")) {
                        columnValues.append(commandTokens[index++]);
                    }
                    columnValues = new StringBuilder(columnValues.toString().replaceAll("\\s+", ""));

                    if (index != commandTokens.length) {
                        StringBuilder condition = new StringBuilder();
                        for (int i = index + 1; i < commandTokens.length; i++) {
                            condition.append(commandTokens[i]);
                        }
                        condition = new StringBuilder(condition.toString().replaceAll("\\s+", ""));

                        tokens = new String[6];
                        tokens[4] = "where";
                        tokens[5] = condition.toString();
                    }
                    tokens[0] = commandTokens[0];
                    tokens[1] = commandTokens[1];
                    tokens[2] = commandTokens[2];
                    tokens[3] = columnValues.toString();

                    lockTables(tokens[1]);
                    if (currentDatabase != null && !currentDatabase.isEmpty()) {
                        update(tokens);
                    } else {
                        System.out.println("No database is selected");
                    }
                    unlockTables(tokens[1]);
                    break;
                case "delete":
                    tokens = new String[3];
                    if (commandTokens.length > 3) {
                        StringBuilder condition = new StringBuilder();
                        for (int i = 4; i < commandTokens.length; i++) {
                            condition.append(commandTokens[i]);
                        }
                        condition = new StringBuilder(condition.toString().replaceAll("\\s+", ""));

                        tokens = new String[5];
                        tokens[3] = "where";
                        tokens[4] = condition.toString();
                    }
                    tokens[0] = "delete";
                    tokens[1] = "from";
                    tokens[2] = commandTokens[2];

                    if (currentDatabase != null && !currentDatabase.isEmpty()) {
                        lockTables(tokens[2]);
                        delete(tokens);
                        unlockTables(tokens[2]);
                    } else {
                        System.out.println("No database is selected");
                    }
                    break;
                case "start":
                    start();
                    break;
                case "commit":
                    commit();
                    break;
                case "rollback":
                    rollback(commandTokens);
                    break;
                case "savepoint":
                    savepoint(commandTokens);
                    break;
            }

            long stopTime = System.nanoTime();
            date = new Date();
            Log.generalLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS").format(date) + ", " +
                    String.join(" ", commandTokens) + ", Duration: " + String.format("%6.6f", (float) (stopTime - startTime) / 1000000) + " milliseconds");
            System.out.println("Command took " + String.format("%6.6f", (float) (stopTime - startTime) / 1000000) + " milliseconds to execute");
        } catch (Exception e) {
            date = new Date();
            Log.eventLog(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS").format(date) +
                    ", Caught exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

/**
 * Class to perform overall execution of the commands
 */
public class Execute {

    /*
     Execute the commands concurrently using threads
     */
    public void executeCommand(String[] commands) {
        Thread[] threads = new Thread[commands.length];
        int i = 0;
        for (String command : commands) {
            // Extract data from between parentheses and format data
            if (command.contains("(") && command.contains(")")) {
                String inBrackets = command.substring(command.indexOf("(") + 1, command.indexOf(")"));
                inBrackets = inBrackets.replace(" ", ":");
                String outBrackets = command.substring(0, command.indexOf("("));
                outBrackets = outBrackets.replaceAll(" +$", "");
                command = outBrackets + " " + inBrackets;
            }
            String[] keys = command.split(" ");

            // Start the execution
            TransactionRun transactionRun = new TransactionRun(keys);
            threads[i] = new Thread(transactionRun);
            threads[i++].start();
        }

        // Wait for the commands to complete
        try {
            for (int j = 0; j < commands.length; j++) {
                threads[j].join();
            }
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
