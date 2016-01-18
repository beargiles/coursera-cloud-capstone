package com.coyotesong.coursera.cloud.support;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

/**
 * Simple unit test that verifies we can read the local test data.
 * 
 * @author bgiles
 */
public class AirportAndCourierTest {
    private static final File ROOT = new File("/media/router/Documents/Coursera Cloud");
    
    @Test
    public void testAirports() throws Exception {
        Set<Integer> ids = new HashSet<>();
        try (Reader r = new FileReader(new File(ROOT, "485012853_T_MASTER_CORD.csv"))) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(1);
                if (id.matches("[0-9]+")) {
                    ids.add(Integer.valueOf(id));
                }
            }
        }
        System.out.println("airports: " + ids.size());
    }
    
    @Test
    public void testCouriers() throws Exception {
        Set<Integer> ids = new HashSet<>();
        try (Reader r = new FileReader(new File(ROOT, "485012853_T_CARRIER_DECODE.csv"))) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(0);
                if (id.matches("[0-9]+")) {
                    ids.add(Integer.valueOf(id));
                }
            }
        }
        System.out.println("couriers: " + ids.size());
    }
}
