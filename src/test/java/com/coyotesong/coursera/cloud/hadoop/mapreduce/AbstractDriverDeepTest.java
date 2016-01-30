package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;

import com.coyotesong.coursera.cloud.domain.AirportInfo;
import com.coyotesong.coursera.cloud.domain.FlightInfo;
import com.coyotesong.coursera.cloud.repository.AirportInfoRepository;
import com.coyotesong.coursera.cloud.repository.FlightInfoRepository;
import com.coyotesong.coursera.cloud.repository.LookupAirportRepository;
import com.coyotesong.coursera.cloud.util.LookupUtil;

/**
 * Deep test class to identify popular airports. This test performs
 * calculations using both traditional SQL queries and map-reduce methods
 * and verifies the results are the same.
 * 
 * @author bgiles
 */
public abstract class AbstractDriverDeepTest {

    @Autowired
    private AirportInfoRepository airportInfoRepository;

    @Autowired
    private FlightInfoRepository flightInfoRepository;
    
    @Autowired
    private LookupAirportRepository lookupAirportRepository;

    static {
        System.setProperty("hadoop.home.dir", "/opt/hadoop");
        try {
            // System.setOut(new PrintStream(new
            // FileOutputStream("/tmp/count.out")));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Load airport information.
     * 
     * @throws Exception
     */
    public void loadAirports(String filename) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
                Reader r = new InputStreamReader(is)) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(0);
                if (id.matches("[0-9]+")) {
                    AirportInfo airport = AirportInfo.CSV.parse(record);
                    airportInfoRepository.save(airport);
                }
            }
        }
    }

    /**
     * Load flight information.
     * 
     * @param filename
     */
    public void loadFlights(String filename) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
                Reader r = new InputStreamReader(is)) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(0);
                if (id.matches("[0-9]+")) {
                    FlightInfo info = FlightInfo.CSV.parse(record);
                    flightInfoRepository.save(info);
                }
            }
        }
    }

    /**
     * Load database so we can compare traditional and MR approach.
     * 
     * @param filename
     * @throws Exception
     */
    public void loadDatabase(String filename) throws Exception {
        // load data conventionally so we can check results later.
        URL url = Thread.currentThread().getContextClassLoader().getResource("rita-static.zip");
        File file = new File(url.getFile());

        LookupUtil.load(file);
        lookupAirportRepository.save(LookupUtil.AIRPORTS.values());
        loadAirports("485012853_T_MASTER_CORD.csv");
        loadFlights(filename); 
    }
}
