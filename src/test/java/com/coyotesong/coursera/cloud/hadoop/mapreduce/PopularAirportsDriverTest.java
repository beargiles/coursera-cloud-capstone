package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.coyotesong.coursera.cloud.hadoop.io.AirportFlightsWritable;

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

    /**
     * Test GatherFlightsMap. It should identify airports and use a value of 1.
     */
    @Test
    public void testGatherFlightsMap() throws IOException {
        final LongWritable inKey = new LongWritable(0); 
        final MapDriver<LongWritable, Text, IntWritable, IntWritable> driver = new MapDriver<>();
        driver.withMapper(new PopularAirportsDriver.GatherFlightsMap());
        driver.withInput(inKey, new Text("2015,1,1,4,2015-01-01,19805,\"N787AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-5.00,\"1230\",7.00,0.00,0.00,"));
        driver.withOutput(new IntWritable(12478), new IntWritable(1));
        driver.withOutput(new IntWritable(12892), new IntWritable(1));
        driver.runTest();
    }
    
    /**
     * Test GatherFlightsReduce. This counts the number of flights by
     * airport.
     */
    @Test
    public void testGatherFlightsReduce() throws IOException {
        final ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable> driver = new ReduceDriver<>();
        driver.withReducer(new PopularAirportsDriver.GatherFlightsReduce());
        driver.withInput(new IntWritable(12478), Arrays.asList(new IntWritable(1), new IntWritable(2)));
        driver.withInput(new IntWritable(12892), Arrays.asList(new IntWritable(1)));
        driver.withOutput(new IntWritable(12478), new IntWritable(3));
        driver.withOutput(new IntWritable(12892), new IntWritable(1));
        driver.runTest();
    }

    /**
     * Test TopAirportsMap. It does nothing but combine the key and value into
     * a new tuple.
     */
    @Test
    public void testTopAirportsMap() throws IOException {
        final MapDriver<Text, Text, NullWritable, AirportFlightsWritable> driver = new MapDriver<>();
        driver.withMapper(new PopularAirportsDriver.TopAirportsMap());
        driver.withInput(new Text("12478"), new Text("3"));
        driver.withOutput(NullWritable.get(), new AirportFlightsWritable(12478, 3));
        driver.runTest();
    }

    /**
     * Test TopAirportsReduce.
     */
    @Test
    public void testTopAirportsReduce() throws IOException {
        final ReduceDriver<NullWritable, AirportFlightsWritable, IntWritable, Text> driver = new ReduceDriver<>();
        driver.withReducer(new PopularAirportsDriver.TopAirportsReduce());
        driver.withInput(NullWritable.get(), Arrays.asList(new AirportFlightsWritable(12478, 3)));
        driver.withOutput(new IntWritable(3), new Text("John F. Kennedy International"));
        driver.runTest();
    }
    
    /**
     * Demonstrate usage of driver.
     * 
     * @throws Exception
     */
    @Test
    @Ignore
    public void demonstrate() throws Exception {
        PopularAirportsDriver driver = new PopularAirportsDriver();
        driver.setConf(new Configuration());

        final File tempdir = Files.createTempDirectory("airlineOntime_").toFile();
        final File workdir = new File(tempdir, "work");

        assertTrue(driver.run(new String[] { ONTIME_FILE.getAbsolutePath(), OUTPUT.getAbsolutePath(), 
                workdir.getAbsolutePath() }) == 1);
    }
}
