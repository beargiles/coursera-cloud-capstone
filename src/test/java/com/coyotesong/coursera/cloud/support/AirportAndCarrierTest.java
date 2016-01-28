package com.coyotesong.coursera.cloud.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

/**
 * Simple unit test that verifies we can read the local test data.
 * 
 * @author bgiles
 */
public class AirportAndCarrierTest {

    @Test
    public void testAirports() throws Exception {
        Map<Integer, String> map = new HashMap<>();
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("485012853_T_MASTER_CORD.csv"); Reader r = new InputStreamReader(is)) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(1);
                if (id.matches("[0-9]+")) {
                    map.put(Integer.valueOf(id), record.get(3));
                }
            }
        }

        assertEquals(6358, map.size());
        assertTrue(map.containsKey(16385));
        assertEquals("Ad-Dabbah Airport", map.get(16385));
    }

    @Test
    public void testCouriers() throws Exception {
        Map<Integer, String> map = new HashMap<>();
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("485012853_T_CARRIER_DECODE.csv"); Reader r = new InputStreamReader(is)) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(0);
                if (id.matches("[0-9]+")) {
                    map.put(Integer.valueOf(id), record.get(3));
                }
            }
        }

        assertEquals(1607, map.size());
        assertTrue(map.containsKey(20500));
        assertEquals("GoJet Airlines LLC d/b/a United Express", map.get(20500));
    }
}
