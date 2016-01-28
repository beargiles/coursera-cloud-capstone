package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Ignore;
import org.junit.Test;

import com.coyotesong.coursera.cloud.hadoop.io.AirlineFlightDelaysWritable;

import static org.junit.Assert.assertTrue;

/**
 * Test class to compare on-time arrival performance.
 * 
 * @author bgiles
 */
public class AirlineOnTimePerformanceDriverDemo {
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
    public void demonstrate() throws Exception {
        final File ROOT = new File("/media/router/Documents/Coursera Cloud");
        final File ONTIME_FILE = new File(ROOT, "360692348_T_ONTIME.csv");

        final AirlineOnTimePerformanceDriver driver = new AirlineOnTimePerformanceDriver();
        driver.setConf(new Configuration());

        final File tempdir = Files.createTempDirectory("airline_ontime_").toFile();
        final File workdir = new File(tempdir, "work");
        final File output = new File(tempdir, "output");

        assertTrue(driver.run(new String[] { ONTIME_FILE.getAbsolutePath(), output.getAbsolutePath(),
                workdir.getAbsolutePath() }) == 1);
    }

}
