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
import com.coyotesong.coursera.cloud.domain.LookupAirport;
import com.coyotesong.coursera.cloud.repository.FlightInfoRepository;
import com.coyotesong.coursera.cloud.repository.LookupAirportRepository;

/**
 * Deep test class to identify popular airports. This test performs
 * calculations using both traditional SQL queries and map-reduce methods
 * and verifies the results are the same.
 * 
 * @author bgiles
 */
@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { DefaultConfig.class,
        RepositoryConfiguration.class }, loader = SpringApplicationContextLoader.class)
public class PopularAirportsDriverDeepTest extends AbstractDriverDeepTest {

    @Autowired
    private FlightInfoRepository flightInfoRepository;
    
    @Autowired
    private LookupAirportRepository lookupAirportRepository;

    /**
     * Check results of first job.
     * @param workdir
     * @throws IOException
     */
    public void checkFirstJob(File workdir) throws IOException {
        final File results = new File(workdir, "part-r-00000");
        assertTrue(results.exists());
        try (Reader r = new FileReader(results); LineNumberReader lnr = new LineNumberReader(r)) {
            String line = null;
            while ((line = lnr.readLine()) != null) {
                String[] values = line.split("\t");
                assertEquals(2, values.length);
                assertTrue(values[0].matches("[0-9]+"));
                assertTrue(values[1].matches("[0-9]+"));
                long count = flightInfoRepository.countByDestAirportId(Integer.parseInt(values[0]));
                assertEquals(count, Integer.parseInt(values[1]));
            }
        }  
    }

    /**
     * Check results of second job. 
     * 
     * @param workdir
     * @throws IOException
     */
    public void checkSecondJob(File output) throws IOException {
        final File results = new File(output, "part-r-00000");
        assertTrue(results.exists());
        try (Reader r = new FileReader(results); LineNumberReader lnr = new LineNumberReader(r)) {
            String line = null;
            
            // find 10 most popular airports and then reverse the order.
            List<Integer> ids = flightInfoRepository.listPopularDestAirportIds();
            ids = ids.subList(0, Math.min(10,ids.size()));

            final Iterator<Integer> iter = ids.iterator();
            while (((line = lnr.readLine()) != null) && iter.hasNext()) {
                final String[] values = line.split("\t");
                assertEquals(2, values.length);
                final Integer id = iter.next();
                final LookupAirport airport = lookupAirportRepository.findOne(id);
                assertEquals(airport.getName(), values[1]);
            }
        }  
    }
 
    /**
     * Deep test of driver.
     * 
     * @throws Exception
     */
    @Test
    @Ignore
    public void test() throws Exception {
        final String filename = "360692348_T_ONTIME.csv";

        // loadDatabase(filename);
        
        // run hadoop job
        final URL url = Thread.currentThread().getContextClassLoader().getResource(filename);

        final PopularAirportsDriver driver = new PopularAirportsDriver();
        final Configuration conf = new Configuration();
        driver.setConf(conf);
        
        final File tempdir = Files.createTempDirectory("popular_airports_").toFile();
        final File workdir = new File(tempdir, "work");
        final File output = new File(tempdir, "output");

        assertTrue(driver.run(new String[] { new File(url.getPath()).getAbsolutePath(), output.getAbsolutePath(),
                workdir.getAbsolutePath() }) == 1);
        
        // verify results of first job
        // checkFirstJob(workdir);
        
        // verify results of second job
        checkSecondJob(output);
    }
}
