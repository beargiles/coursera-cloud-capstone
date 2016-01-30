package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;

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
import com.coyotesong.coursera.cloud.domain.AirlineFlightDelays;
import com.coyotesong.coursera.cloud.repository.FlightInfoRepository;

/**
 * Test class to compare on-time arrival performance.
 * 
 * @author bgiles
 */
@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { DefaultConfig.class,
        RepositoryConfiguration.class }, loader = SpringApplicationContextLoader.class)
public class CarrierOnTimePerformanceDriverDeepTest extends AbstractDriverDeepTest {

    @Autowired
    private FlightInfoRepository flightInfoRepository;

    /**
     * Check results of first job.
     * @param workdir
     * @throws IOException
     */
    public void checkFirstJob(File workdir) throws IOException {
        final File results = new File(workdir, "part-r-00000");
        assertTrue(results.exists());
        try (Reader r = new FileReader(results); LineNumberReader lnr = new LineNumberReader(r)) {
            
            final List<AirlineFlightDelays> delays = flightInfoRepository.listArrivalFlightDelaysByAirlineAsAirlineFlightDelays();
            final Iterator<AirlineFlightDelays> iter = delays.iterator();

            String line = null;
            while (((line = lnr.readLine()) != null) && iter.hasNext()) {
                final AirlineFlightDelays delay = iter.next();
                final String[] values = line.split("[\t,]");
                assertEquals(5, values.length);
                for (int i = 0; i < values.length; i++) {
                    assertTrue(values[i].matches("-?[0-9]+"));
                }
                assertEquals(delay.getAirlineId(), Integer.parseInt(values[0]));
                assertEquals(delay.getNumFlights(), Integer.parseInt(values[1]));
                assertEquals(delay.getDelay(), Integer.parseInt(values[2]));
                assertEquals(delay.getDelaySquared(), Integer.parseInt(values[3]));
                assertEquals(delay.getMaxDelay(), Integer.parseInt(values[4]));
            }
        }  
    }

    @Test
    // @Ignore
    public void test() throws Exception {
        final String filename = "360692348_T_ONTIME.csv";

        // loadDatabase(filename);

        // run hadoop job
        final URL url = Thread.currentThread().getContextClassLoader().getResource(filename);

        final CarrierOnTimePerformanceDriver driver = new CarrierOnTimePerformanceDriver();
        driver.setConf(new Configuration());

        final File tempdir = Files.createTempDirectory("carrier_ontime_").toFile();
        final File workdir = new File(tempdir, "work");
        final File output = new File(tempdir, "output");
        assertTrue(driver.run(new String[] { new File(url.getPath()).getAbsolutePath(), output.getAbsolutePath(),
                workdir.getAbsolutePath() }) == 1);
        
        // test results.
        checkFirstJob(workdir);
        
        // how to check second job?
    }
}
