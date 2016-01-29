package com.coyotesong.coursera.cloud;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;

import com.coyotesong.coursera.cloud.domain.AirlineInfo;
import com.coyotesong.coursera.cloud.domain.AirportInfo;
import com.coyotesong.coursera.cloud.repository.AirlineInfoRepository;
import com.coyotesong.coursera.cloud.repository.AirportInfoRepository;
import com.coyotesong.coursera.cloud.repository.LookupAirlineRepository;
import com.coyotesong.coursera.cloud.repository.LookupAirportRepository;
import com.coyotesong.coursera.cloud.util.LookupUtil;

/**
 * Spring boot app to initialize static database content. This is
 * useful during initial development but the final application will
 * want to used deployed CachedFiles.
 * 
 * Note: the .csv files were downloaded from RITA and the columns
 * may change in the future. They are located in test resources and
 * not deployed.
 * 
 * @author bgiles
 */
@EnableAutoConfiguration
public class InitializeStaticDatabaseApplication {
    
    @Autowired
    private AirlineInfoRepository airlineInfoRepository;
    
    @Autowired
    private AirportInfoRepository airportInfoRepository;
    
    @Autowired
    private LookupAirlineRepository lookupAirlineRepository;
    
    @Autowired
    private LookupAirportRepository lookupAirportRepository;
    
    /**
     * Load airline information.
     * 
     * @throws Exception
     */
    public void loadAirlines(String filename) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(filename); Reader r = new InputStreamReader(is)) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(0);
                if (id.matches("[0-9]+")) {
                    AirlineInfo airline = AirlineInfo.CSV.parse(record);
                    airlineInfoRepository.save(airline);
                }
            }
        }
    }
    
    /**
     * Load airport information.
     * 
     * @throws Exception
     */
    public void loadAirports(String filename) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(filename); Reader r = new InputStreamReader(is)) {
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
     * Load static data into database.
     * 
     * @throws Exception
     */
    public void load() throws Exception {
        // store dynamic lookup data. This has much more information than
        // static lookup data.
        loadAirlines("485012853_T_CARRIER_DECODE.csv");
        loadAirports("485012853_T_MASTER_CORD.csv");

        // store static lookup data.
        URL url = Thread.currentThread().getContextClassLoader().getResource("rita-static.zip");
        File file = new File(url.getFile());
        LookupUtil.load(file);

        lookupAirlineRepository.save(LookupUtil.AIRLINES.values());
        lookupAirportRepository.save(LookupUtil.AIRPORTS.values());
    }
    
	public static void main(String[] args) throws Exception {
		ApplicationContext ctx = SpringApplication.run(InitializeStaticDatabaseApplication.class, args);

		InitializeStaticDatabaseApplication app = (InitializeStaticDatabaseApplication) ctx.getBean("initializeStaticDatabaseApplication");
		app.load();
	}
}
