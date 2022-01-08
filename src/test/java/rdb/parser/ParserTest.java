package rdb.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ParserTest {
    Parser parser = new Parser();

    @Test
    public void parseCommandTest() {
        String[] commands = parser.parseCommand("create database user;create erd user");
        assertEquals("create database user", commands[0], "Command does not match");
        assertEquals("create erd user", commands[1], "Command does not match");
    }

    @Test
    public void removeSpacesTest() {
        assertEquals("create erd user", parser.removeSpaces(" create  erd  user      "),
                "Spaces have not been removed");
    }
}
