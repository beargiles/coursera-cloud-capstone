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

import com.coyotesong.coursera.cloud.domain.CarrierInfo;
import com.coyotesong.coursera.cloud.domain.AirportInfo;

/**
 * Simple unit test that verifies we can read the local test data.
 * 
 * @author bgiles
 */
public class AirportAndCarrierTest {

    @Test
    public void testAirports() throws Exception {
        Map<Integer, AirportInfo> map = new HashMap<>();
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("485012853_T_MASTER_CORD.csv"); Reader r = new InputStreamReader(is)) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(0);
                if (id.matches("[0-9]+")) {
                    AirportInfo airport = AirportInfo.CSV.parse(record);
                    map.put(airport.getId(), airport);
                }
            }
        }

        assertEquals(13137, map.size());
        System.out.println(map.keySet().iterator().next());
        assertTrue(map.containsKey(1245201));
        assertEquals("Boston City Heliport", map.get(1245201).getDisplayAirportName());
    }

    @Test
    public void testCarriers() throws Exception {
        Map<Integer, CarrierInfo> map = new HashMap<>();
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("485012853_T_CARRIER_DECODE.csv"); Reader r = new InputStreamReader(is)) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(0);
                if (id.matches("[0-9]+")) {
                    CarrierInfo airline = CarrierInfo.CSV.parse(record);
                    map.put(airline.getAirlineId(), airline);
                }
            }
        }

        assertEquals(1607, map.size());
        assertTrue(map.containsKey(20500));
        assertEquals("GoJet Airlines LLC d/b/a United Express", map.get(20500).getCarrierName());
    }
}
