package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationContextLoader;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.coyotesong.coursera.cloud.configuration.DefaultConfig;
import com.coyotesong.coursera.cloud.configuration.RepositoryConfiguration;
import com.coyotesong.coursera.cloud.domain.FlightInfo;
import com.coyotesong.coursera.cloud.repository.FlightInfoRepository;
import com.coyotesong.coursera.cloud.util.LookupUtil;

/**
 * Test class to compare on-time arrival performance.
 * 
 * @author bgiles
 */
@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { DefaultConfig.class,
        RepositoryConfiguration.class }, loader = SpringApplicationContextLoader.class)
public class AirportOnTimePerformanceDriverDeepTest {

    @Autowired
    private FlightInfoRepository flightInfoRepository;

    static {
        System.setProperty("hadoop.home.dir", "/opt/hadoop");
        try {
            // System.setOut(new PrintStream(new
            // FileOutputStream("/tmp/count.out")));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public void loadOntime(String filename) throws Exception {
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
        loadOntime(filename);
    }

    @Test
    //@Ignore
    public void test() throws Exception {
        final String filename = "360692348_T_ONTIME.csv";

        loadDatabase(filename);

        // run hadoop job
        final URL url = Thread.currentThread().getContextClassLoader().getResource(filename);

        final AirportOnTimePerformanceDriver driver = new AirportOnTimePerformanceDriver();
        driver.setConf(new Configuration());

        final File tempdir = Files.createTempDirectory("airport_ontime_").toFile();
        final File workdir = new File(tempdir, "work");
        final File output = new File(tempdir, "output");
        assertTrue(driver.run(new String[] { new File(url.getPath()).getAbsolutePath(), output.getAbsolutePath(),
                workdir.getAbsolutePath() }) == 1);
        
        // TODO implement tests of results.
    }
}
