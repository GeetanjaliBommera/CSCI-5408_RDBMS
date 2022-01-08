package rdb.syntaxchecker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SyntaxTest {
    Syntax syntax = new Syntax();

    @Test
    public void useTest() {
        assertTrue(syntax.use(new String[]{"use", "data"}), "rdb.syntaxchecker.Syntax for use command failed");
        assertFalse(syntax.use(new String[]{"use", "data@",}), "rdb.syntaxchecker.Syntax for use command passed");
        assertFalse(syntax.use(new String[]{"use", "data", "1"}), "rdb.syntaxchecker.Syntax for use command passed");
    }

    @Test
    public void createTest() {
        assertTrue(syntax.create(new String[]{"create", "erd", "data"}), "rdb.syntaxchecker.Syntax for create command failed");
        assertTrue(syntax.create(new String[]{"create", "dump", "data"}), "rdb.syntaxchecker.Syntax for create command failed");
        assertTrue(syntax.create(new String[]{"create", "database", "data"}), "rdb.syntaxchecker.Syntax for create command failed");
        assertFalse(syntax.create(new String[]{"create", "erd", "d@ta"}), "rdb.syntaxchecker.Syntax for create command passed");
        assertFalse(syntax.create(new String[]{"create", "dump", "d@ta"}), "rdb.syntaxchecker.Syntax for create command passed");
        assertFalse(syntax.create(new String[]{"create", "database", "d@ta"}), "rdb.syntaxchecker.Syntax for create command passed");
        assertFalse(syntax.create(new String[]{"create", "database"}), "rdb.syntaxchecker.Syntax for create command passed");
        assertFalse(syntax.create(new String[]{"create", "erd"}), "rdb.syntaxchecker.Syntax for create command passed");
        assertFalse(syntax.create(new String[]{"create", "dump"}), "rdb.syntaxchecker.Syntax for create command passed");
        assertFalse(syntax.create(new String[]{"create", "db", "data"}), "rdb.syntaxchecker.Syntax for create command passed");
    }

    @Test
    public void dropTest() {
        assertTrue(syntax.drop(new String[]{"drop", "table", "data"}), "rdb.syntaxchecker.Syntax for drop command failed");
        assertTrue(syntax.drop(new String[]{"drop", "database", "data"}), "rdb.syntaxchecker.Syntax for drop command failed");
        assertFalse(syntax.drop(new String[]{"drop", "table", "d@ta"}), "rdb.syntaxchecker.Syntax for drop command passed");
        assertFalse(syntax.drop(new String[]{"drop", "database", "d@ta"}), "rdb.syntaxchecker.Syntax for drop command passed");
        assertFalse(syntax.drop(new String[]{"drop", "table"}), "rdb.syntaxchecker.Syntax for drop command passed");
        assertFalse(syntax.drop(new String[]{"drop", "database"}), "rdb.syntaxchecker.Syntax for drop command passed");
        assertFalse(syntax.drop(new String[]{"drop", "db", "data"}), "rdb.syntaxchecker.Syntax for drop command passed");
    }

    @Test
    public void startTest() {
        assertTrue(syntax.start(new String[]{"start", "transaction"}), "rdb.syntaxchecker.Syntax for start command failed");
        assertFalse(syntax.start(new String[]{"start"}), "rdb.syntaxchecker.Syntax for start command passed");
        assertFalse(syntax.start(new String[]{"start", "transaction", "data"}), "rdb.syntaxchecker.Syntax for start command passed");
    }

    @Test
    public void commitTest() {
        assertTrue(syntax.commit(new String[]{"commit"}), "rdb.syntaxchecker.Syntax for commit command failed");
        assertFalse(syntax.commit(new String[]{"commit", "data"}), "rdb.syntaxchecker.Syntax for commit command passed");
    }

    @Test
    public void rollbackTest() {
        assertTrue(syntax.rollback(new String[]{"rollback"}), "rdb.syntaxchecker.Syntax for rollback command failed");
        assertTrue(syntax.rollback(new String[]{"rollback", "to", "point"}), "rdb.syntaxchecker.Syntax for rollback command failed");
        assertFalse(syntax.rollback(new String[]{"rollback", "1"}), "rdb.syntaxchecker.Syntax for rollback command passed");
        assertFalse(syntax.rollback(new String[]{"rollback", "to", "point", "point"}),
                "rdb.syntaxchecker.Syntax for rollback command passed");
    }

    @Test
    public void savepointTest() {
        assertTrue(syntax.savepoint(new String[]{"savepoint", "point"}), "rdb.syntaxchecker.Syntax for savepoint command failed");
        assertFalse(syntax.savepoint(new String[]{"savepoint", "p*int"}), "rdb.syntaxchecker.Syntax for savepoint command passed");
        assertFalse(syntax.savepoint(new String[]{"savepoint", "point", "point"}),
                "rdb.syntaxchecker.Syntax for savepoint command passed");
    }

    @Test
    public void exitTest() {
        assertTrue(syntax.exit(new String[]{"exit"}), "rdb.syntaxchecker.Syntax for exit command failed");
        assertFalse(syntax.exit(new String[]{"exit", "1"}), "rdb.syntaxchecker.Syntax for exit command passed");
    }

    @Test
    public void keywordsTest() {
        assertTrue(syntax.keywords(new String[]{"create", "erd", "data"}), "rdb.syntaxchecker.Syntax for create command failed");
        assertFalse(syntax.keywords(new String[]{"crete", "erd", "data"}), "rdb.syntaxchecker.Syntax for unknown command passed");
    }

    @Test
    public void keywordCheckTest() {
        assertTrue(syntax.keywordCheck("create erd data"), "rdb.syntaxchecker.Syntax test failed");
        assertFalse(syntax.keywordCheck("create erd data 1"), "rdb.syntaxchecker.Syntax test failed");
    }

    @Test
    public void checkTest() {
        assertTrue(syntax.check(new String[]{"create database data", "create erd data"}), "rdb.syntaxchecker.Syntax check failed");
        assertFalse(syntax.check(new String[]{"create data data", "create erd data"}), "rdb.syntaxchecker.Syntax check failed");
    }
}
