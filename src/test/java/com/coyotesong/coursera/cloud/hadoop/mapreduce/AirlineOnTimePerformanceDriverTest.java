package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
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
public class AirlineOnTimePerformanceDriverTest {
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
     * Test GatherArrivalDelayMap. It should extract the airline ID and delay
     * information.
     */
    @Test
    public void testArrivalDelayMap() throws IOException {
        final MapDriver<LongWritable, Text, IntWritable, AirlineFlightDelaysWritable> driver = new MapDriver<>();
        driver.withMapper(new AirlineOnTimePerformanceDriver.GatherArrivalDelayMap());
        driver.withInput(new LongWritable(0), new Text(
                "2015,1,1,4,2015-01-01,19805,\"N787AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-5.00,\"1230\",7.00,0.00,0.00,"));
        driver.withInput(new LongWritable(1), new Text(
                "2015,1,2,5,2015-01-02,19805,\"N795AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-10.00,\"1230\",-19.00,0.00,0.00,"));
        driver.withOutput(new IntWritable(19805), new AirlineFlightDelaysWritable(19805, 7));
        driver.withOutput(new IntWritable(19805), new AirlineFlightDelaysWritable(19805, -19));
        driver.runTest();
    }

    /**
     * Test GatherArrivalDelayMap - it should not consider cancelled flights.
     */
    @Test
    public void testArrivalDelayMapCancelledFlight() throws IOException {
        final MapDriver<LongWritable, Text, IntWritable, AirlineFlightDelaysWritable> driver = new MapDriver<>();
        driver.withMapper(new AirlineOnTimePerformanceDriver.GatherArrivalDelayMap());
        driver.withInput(new LongWritable(0), new Text(
                "2015,1,1,4,2015-01-01,19805,\"N787AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-5.00,\"1230\",7.00,1.00,0.00,"));
        driver.runTest();
    }

    /**
     * Test GatherArrivalDelayMap - it should not consider diverted flights.
     */
    @Test
    public void testArrivalDelayMapDivertedFlight() throws IOException {
        final MapDriver<LongWritable, Text, IntWritable, AirlineFlightDelaysWritable> driver = new MapDriver<>();
        driver.withMapper(new AirlineOnTimePerformanceDriver.GatherArrivalDelayMap());
        driver.withInput(new LongWritable(0), new Text(
                "2015,1,1,4,2015-01-01,19805,\"N787AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-5.00,\"1230\",7.00,0.00,1.00,"));
        driver.runTest();
    }

    /**
     * Test GatherArrivalDelayReduce. It should combine the arrival delay
     * information.
     */
    @Test
    public void testArrivalDelayReduce() throws IOException {
        final ReduceDriver<IntWritable, AirlineFlightDelaysWritable, IntWritable, AirlineFlightDelaysWritable> driver = new ReduceDriver<>();
        driver.withReducer(new AirlineOnTimePerformanceDriver.GatherArrivalDelayReduce());
        driver.withInput(new IntWritable(19805),
                Arrays.asList(new AirlineFlightDelaysWritable(19805, 7), new AirlineFlightDelaysWritable(19805, -19)));
        driver.withOutput(new IntWritable(19805), new AirlineFlightDelaysWritable(19805, new int[] { 2, -12, 410, 7 }));
        driver.runTest();
    }

    /**
     * Test CompareArrivalDelayMap. It does nothing but combine the key and value into
     * a new tuple.
     */
    @Test
    public void testCompareArrivalDelayMap() throws IOException {
        final MapDriver<Text, Text, IntWritable, AirlineFlightDelaysWritable> driver = new MapDriver<>();
        driver.withMapper(new AirlineOnTimePerformanceDriver.CompareArrivalDelayMap());
        driver.withInput(new Text("19805"), new Text("2,-12,410,7"));
        driver.withOutput(new IntWritable(19805), new AirlineFlightDelaysWritable(19805, new int[] { 2, -12, 410, 7 }));
        driver.runTest();
    }
    
    /**
     * Test CompareArrivalDelayReduce.
     */
    @Test
    public void testCompareArrivalDelayReduce() throws IOException {
        final ReduceDriver<IntWritable, AirlineFlightDelaysWritable, NullWritable, Text> driver = new ReduceDriver<>();
        driver.withReducer(new AirlineOnTimePerformanceDriver.CompareArrivalDelayReduce());
        driver.withInput(new IntWritable(19805), Arrays.asList(new AirlineFlightDelaysWritable(19805, new int[] { 2, -12, 410, 7 })));
        driver.withOutput(NullWritable.get(), new Text(" 30.770 -6.000 American Airlines Inc."));
        driver.runTest();
    }
    
    /**
     * Test CompareArrivalDelayReduce - verify only the top 25 airlines
     * are compared.
     * 
     * @throws Exception
     */
    @Test
    @Ignore
    public void testCompareArrivalDelayReduceTopAirlines() throws IOException {
        final ReduceDriver<IntWritable, AirlineFlightDelaysWritable, NullWritable, Text> driver = new ReduceDriver<>();
        // FIXME: implement
    }
    
    @Test
    @Ignore
    public void demonstrate() throws Exception {
        final AirlineOnTimePerformanceDriver driver = new AirlineOnTimePerformanceDriver();
        driver.setConf(new Configuration());

        final File tempdir = Files.createTempDirectory("airlineOntime_").toFile();
        final File workdir = new File(tempdir, "work");

        assertTrue(driver.run(new String[] { ONTIME_FILE.getAbsolutePath(), OUTPUT.getAbsolutePath(),
                workdir.getAbsolutePath() }) == 1);
    }

}
