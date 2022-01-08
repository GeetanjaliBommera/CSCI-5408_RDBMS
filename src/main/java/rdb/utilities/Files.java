package rdb.utilities;

import java.io.*;

/**
 * Class which performs file operations
 */
public class Files {

    /*
     Creates a new file in the specified folder
     */
    public File createFile(String folderName, String fileName) {
        File file = null;
        try {
            file = new File("./" + folderName, fileName);
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    /*
     Gets list of files under the Databases folder with specific extension
     */
    public File[] getFileList(String database, String endsWith) {
        File directory = new File("./Databases/" + database);
        File[] files = directory.listFiles((dir, filename) -> filename.endsWith(endsWith));

        assert files != null;
        return files;
    }

    /*
     Creates and appends line of text to the specified file
     */
    public void writeFile(File file, String text) {
        try {
            FileWriter writer = new FileWriter(file, true);
            BufferedWriter bw = new BufferedWriter(writer);
            bw.write(text);
            bw.newLine();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     Reads the entire contents of the specified file
     */
    public String readFile(File file) {
        String content = null;
        try {
            FileReader fileReader = new FileReader(file);
            char[] chars = new char[(int) file.length()];
            fileReader.read(chars);
            content = new String(chars);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return content;
    }
}
