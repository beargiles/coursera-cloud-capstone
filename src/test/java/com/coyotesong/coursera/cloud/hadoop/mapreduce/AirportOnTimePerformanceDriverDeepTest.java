package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;

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
public class AirportOnTimePerformanceDriverDeepTest extends AbstractDriverDeepTest {

    @Autowired
    private FlightInfoRepository flightInfoRepository;
 
    @Test
    @Ignore
    public void test() throws Exception {
        final String filename = "360692348_T_ONTIME.csv";

        // loadDatabase(filename);

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
