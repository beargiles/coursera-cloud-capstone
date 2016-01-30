package com.coyotesong.coursera.cloud.hadoop.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

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

/**
 * Test class to compare on-time arrival performance.
 * 
 * @author bgiles
 */
public class CarrierOnTimePerformanceDriverTest {
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
        driver.withMapper(new CarrierOnTimePerformanceDriver.GatherArrivalDelayMap());
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
        driver.withMapper(new CarrierOnTimePerformanceDriver.GatherArrivalDelayMap());
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
        driver.withMapper(new CarrierOnTimePerformanceDriver.GatherArrivalDelayMap());
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
        driver.withReducer(new CarrierOnTimePerformanceDriver.GatherArrivalDelayReduce());
        driver.withInput(new IntWritable(19805),
                Arrays.asList(new AirlineFlightDelaysWritable(19805, 7), new AirlineFlightDelaysWritable(19805, -19)));
        driver.withOutput(new IntWritable(19805), new AirlineFlightDelaysWritable(19805, new int[] { 2, -12, 410, 7 }));
        driver.runTest();
    }

    /**
     * Test GatherArrivalDelayMap and GatherArrvalDelayReduce.
     */
    @Test
    public void testGatherArrivalDelayMapReduce() throws IOException {
        final CarrierOnTimePerformanceDriver.GatherArrivalDelayMap mapper = new CarrierOnTimePerformanceDriver.GatherArrivalDelayMap();
        final CarrierOnTimePerformanceDriver.GatherArrivalDelayReduce reducer = new CarrierOnTimePerformanceDriver.GatherArrivalDelayReduce();
        final MapReduceDriver<LongWritable, Text, IntWritable, AirlineFlightDelaysWritable, IntWritable, AirlineFlightDelaysWritable> driver = new MapReduceDriver<>(
                mapper, reducer);
        driver.withInput(new LongWritable(0), new Text(
                "2015,1,1,4,2015-01-01,19805,\"N787AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-5.00,\"1230\",7.00,0.00,0.00,"));
        driver.withInput(new LongWritable(1), new Text(
                "2015,1,2,5,2015-01-02,19805,\"N795AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-10.00,\"1230\",-19.00,0.00,0.00,"));
        driver.withOutput(new IntWritable(19805), new AirlineFlightDelaysWritable(19805, new int[] { 2, -12, 410, 7 }));
        driver.runTest();
    }

    /**
     * Test CompareArrivalDelayMap. It does nothing but combine the key and
     * value into a new tuple.
     * @throws URISyntaxException 
     */
    @Test
    public void testCompareArrivalDelayMap() throws IOException {
        final MapDriver<Text, Text, IntWritable, AirlineFlightDelaysWritable> driver = new MapDriver<>();
        driver.withMapper(new CarrierOnTimePerformanceDriver.CompareArrivalDelayMap());
        driver.withInput(new Text("19805"), new Text("2,-12,410,7"));
        driver.withOutput(new IntWritable(19805), new AirlineFlightDelaysWritable(19805, new int[] { 2, -12, 410, 7 }));
        driver.runTest();
    }

    /**
     * Test CompareArrivalDelayReduce.
     * @throws URISyntaxException 
     */
    @Test
    public void testCompareArrivalDelayReduce() throws IOException, URISyntaxException {
        final ReduceDriver<IntWritable, AirlineFlightDelaysWritable, NullWritable, Text> driver = new ReduceDriver<>();
        final URI uri = this.getClass().getClassLoader().getResource("rita-static.zip").toURI();
        driver.setCacheArchives(new URI[] { uri });

        driver.withReducer(new CarrierOnTimePerformanceDriver.CompareArrivalDelayReduce());
        driver.withInput(new IntWritable(19805),
                Arrays.asList(new AirlineFlightDelaysWritable(19805, new int[] { 2, -12, 410, 7 })));
        driver.withOutput(NullWritable.get(), new Text(" 30.770 -6.000 American Airlines Inc."));
        driver.runTest();
    }

    /**
     * Test GatherArrivalDelayMap and GatherArrvalDelayReduce.
     * 
     * @throws URISyntaxException
     */
    @Test
    public void testCompareArrivalDelayMapReduce() throws IOException, URISyntaxException {
        final CarrierOnTimePerformanceDriver.CompareArrivalDelayMap mapper = new CarrierOnTimePerformanceDriver.CompareArrivalDelayMap();
        final CarrierOnTimePerformanceDriver.CompareArrivalDelayReduce reducer = new CarrierOnTimePerformanceDriver.CompareArrivalDelayReduce();
        final MapReduceDriver<Text, Text, IntWritable, AirlineFlightDelaysWritable, NullWritable, Text> driver = new MapReduceDriver<>(
                mapper, reducer);
        final URI uri = this.getClass().getClassLoader().getResource("rita-static.zip").toURI();
        driver.setCacheArchives(new URI[] { uri });

        driver.withInput(new Text("19805"), new Text("2,-12,410,7"));
        driver.withOutput(NullWritable.get(), new Text(" 30.770 -6.000 American Airlines Inc."));
        driver.runTest();
    }

    /**
     * Test CompareArrivalDelayReduce - verify only the top 25 airlines are
     * compared.
     * 
     * @throws Exception
     */
    @Test
    @Ignore
    public void testCompareArrivalDelayReduceTopAirlines() throws IOException {
        final ReduceDriver<IntWritable, AirlineFlightDelaysWritable, NullWritable, Text> driver = new ReduceDriver<>();
        // FIXME: implement
    }
}
