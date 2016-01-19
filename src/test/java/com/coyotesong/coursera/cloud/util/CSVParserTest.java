package com.coyotesong.coursera.cloud.util;

import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for CSV parser.
 * 
 * @author bgiles
 */
public class CSVParserTest {

    @Test
    public void test1() {
        List<String> x = CSVParser.parse("");
        assertTrue(x.isEmpty());       
    }

    @Test
    public void test2() {
        List<String> x = CSVParser.parse("a");
        assertEquals(1, x.size());
        assertEquals("a", x.get(0));
    }

    @Test
    public void test3() {
        List<String> x = CSVParser.parse("\"a\"");
        assertEquals(1, x.size());
        assertEquals("a", x.get(0));
    }

    @Test
    public void test4() {
        List<String> x = CSVParser.parse("a,b");
        assertEquals(2, x.size());
        assertEquals("a", x.get(0));
        assertEquals("b", x.get(1));
    }

    @Test
    public void test5() {
        List<String> x = CSVParser.parse("\"a,b\"");
        assertEquals(1, x.size());
        assertEquals("a,b", x.get(0));
    }

    @Test
    public void test6() {
        List<String> x = CSVParser.parse("a\\,b");
        assertEquals(1, x.size());
        assertEquals("a,b", x.get(0));
    }

    @Test
    public void test7() {
        List<String> x = CSVParser.parse("a\\\"b");
        assertEquals(1, x.size());
        assertEquals("a\"b", x.get(0));
    }

    @Test
    public void test8() {
        List<String> x = CSVParser.parse("a\\\\b");
        assertEquals(1, x.size());
        assertEquals("a\\b", x.get(0));
    }

    @Test
    public void test9() {
        List<String> x = CSVParser.parse("a,,b");
        assertEquals(3, x.size());
        assertEquals("a", x.get(0));
        assertEquals("", x.get(1));
        assertEquals("b", x.get(2));
    }

    @Test
    public void test10() {
        List<String> x = CSVParser.parse("a,");
        assertEquals(2, x.size());
        assertEquals("a", x.get(0));
        assertEquals("", x.get(1));
    }

    @Test
    public void test11() {
        List<String> x = CSVParser.parse(",b");
        assertEquals(2, x.size());
        assertEquals("", x.get(0));
        assertEquals("b", x.get(1));
    }

    @Test
    public void test12() {
        List<String> x = CSVParser.parse(",");
        assertEquals(2, x.size());
        assertEquals("", x.get(0));
        assertEquals("", x.get(1));
    }
}
