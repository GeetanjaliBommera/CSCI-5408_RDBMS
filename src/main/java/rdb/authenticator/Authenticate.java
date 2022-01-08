package rdb.authenticator;

import rdb.utilities.Files;

import java.io.File;
import java.util.Scanner;

public class Authenticate {

    public Files files;

    public File filename;

    public String username, password, securityAnswer;

    public Authenticate() {
        files = new Files();
        files.createFile("Credentials", "CredentialDetails.txt");
        filename = new File("./Credentials/CredentialDetails.txt");
        Boolean result;
        do {
            result = login();
        } while (result == false);
    }

    public boolean login() {
        System.out.println("Please enter username");
        Scanner scan = new Scanner(System.in);
        username = scan.next();
        System.out.println("Please enter password");
        password = scan.next();
        String dataInFile = files.readFile(filename);
        if (dataInFile.contains("Username: " + username + " Password: " + password + " @")) {
            System.out.println("Please answer your security question");
            int i = dataInFile.indexOf("QuestionTo" + username+ ":");
            int j = dataInFile.indexOf(" Answer" + username);
            String question = dataInFile.substring(i, j);
            System.out.println(question);
            securityAnswer = scan.next();
            String result = question + " Answer" + username + ": " + securityAnswer + " " + "@";
            if (dataInFile.contains(result)) {
                System.out.println("Login Successful");
                return true;
            } else {
                System.out.println("Incorrect answer for your security question. Sorry! Please try again");
                return false;
            }
        } else {
            System.out.println("Incorrect username or password! Please try again or create");
            return false;
        }
    }

}
