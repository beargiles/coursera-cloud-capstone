package com.coyotesong.coursera.cloud.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;

import com.coyotesong.coursera.cloud.domain.LookupAirline;
import com.coyotesong.coursera.cloud.domain.LookupAirport;

import static org.junit.Assert.*;

/**
 * Verify static data loader. These are mostly smoke tests.
 * 
 * @author bgiles
 */
public class LookupUtilTest {
    
    @BeforeClass
    public static void setup() throws IOException {
        URL url = LookupUtilTest.class.getClassLoader().getResource("rita-static.zip");
        File file = new File(url.getFile());
        assertTrue(file.exists());
        LookupUtil.load(file);
    }
    
    @Test
    public void testAirlines() {
        assertEquals(1607, LookupUtil.AIRLINES.size());

        final LookupAirline united = LookupUtil.AIRLINES.get(19977);
        assertEquals("United Air Lines Inc.", united.getName());
        assertEquals("UA", united.getAbbreviation());

        final LookupAirline frontier = LookupUtil.AIRLINES.get(20436);
        assertEquals("Frontier Airlines Inc.", frontier.getName());
        assertEquals("F9", frontier.getAbbreviation());
    }
    
    @Test
    public void testAirports() {
        assertEquals(6358, LookupUtil.AIRPORTS.size());
        
        final LookupAirport denver = LookupUtil.AIRPORTS.get(11292);
        assertEquals("Denver International", denver.getName());
        assertEquals("Denver, CO", denver.getLocation());
    }
}
