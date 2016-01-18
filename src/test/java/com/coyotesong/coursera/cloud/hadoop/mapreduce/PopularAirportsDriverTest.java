package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test class to identify popular airports.
 * 
 * @author bgiles
 */
public class PopularAirportsDriverTest {
    private static final File ROOT = new File("/media/router/Documents/Coursera Cloud");
    private static final File ONTIME_FILE = new File(ROOT, "360692348_T_ONTIME.csv");
    private static final File OUTPUT = new File("/tmp/count");

    static {
        System.setProperty("hadoop.home.dir", "/opt/hadoop");
        try {
            // System.setOut(new PrintStream(new
            // FileOutputStream("/tmp/count.out")));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Test
    @Ignore
    public void test() throws Exception {
        PopularAirportsDriver driver = new PopularAirportsDriver();
        driver.setConf(new Configuration());

        final File tempdir = Files.createTempDirectory("airlineOntime_").toFile();
        final File workdir = new File(tempdir, "work");

        assertTrue(driver.run(new String[] { ONTIME_FILE.getAbsolutePath(), OUTPUT.getAbsolutePath(), 
                workdir.getAbsolutePath() }) == 1);
    }

}
