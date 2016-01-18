package com.coyotesong.coursera.cloud;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

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

@EnableAutoConfiguration
public class InitializeStaticDatabaseApplication {
    private static final File ROOT = new File("/media/router/Documents/Coursera Cloud");
    
    @Autowired
    private AirlineInfoRepository airlineInfoRepository;
    
    @Autowired
    private AirportInfoRepository airportInfoRepository;
    
    /**
     * Load airline information.
     * 
     * @throws Exception
     */
    public void loadAirlines() throws Exception {
        try (Reader r = new FileReader(new File(ROOT, "485012853_T_CARRIER_DECODE.csv"))) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(0);
                if (id.matches("[0-9]+")) {
                    AirlineInfo info = new AirlineInfo(Integer.valueOf(id), record.get(3));
                    airlineInfoRepository.save(info);
                }
            }
        }
        System.err.println("airlines: " + airlineInfoRepository.count());
    }
    
    /**
     * Load airport information.
     * 
     * @throws Exception
     */
    public void loadAirports() throws Exception {
        try (Reader r = new FileReader(new File(ROOT, "485012853_T_MASTER_CORD.csv"))) {
            for (CSVRecord record : CSVFormat.EXCEL.parse(r)) {
                String id = record.get(0);
                if (id.matches("[0-9]+")) {
                    AirportInfo info = new AirportInfo(Integer.valueOf(id), record.get(3), record.get(4),
                            record.get(8), record.get(6));
                    airportInfoRepository.save(info);
                }
            }
        }
        System.err.println("airports: " + airportInfoRepository.count());
    }

    /**
     * Load static data into database.
     * 
     * @throws Exception
     */
    public void load() throws Exception {
        loadAirlines();
        loadAirports();
    }
    
	public static void main(String[] args) throws Exception {
		ApplicationContext ctx = SpringApplication.run(InitializeStaticDatabaseApplication.class, args);

		InitializeStaticDatabaseApplication app = (InitializeStaticDatabaseApplication) ctx.getBean("initializeStaticDatabaseApplication");
		app.load();	
	}
}
