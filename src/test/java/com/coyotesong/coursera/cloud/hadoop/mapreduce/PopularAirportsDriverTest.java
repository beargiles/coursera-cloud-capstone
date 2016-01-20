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
import org.junit.Test;

import com.coyotesong.coursera.cloud.hadoop.io.AirportFlightsWritable;

/**
 * Test class to identify popular airports.
 * 
 * TODO: MRUnit pipeline tests?
 * 
 * @author bgiles
 */
public class PopularAirportsDriverTest {
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
        driver.withInput(inKey, new Text(
                "2015,1,1,4,2015-01-01,19805,\"N787AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-5.00,\"1230\",7.00,0.00,0.00,"));
        driver.withOutput(new IntWritable(12478), new IntWritable(1));
        driver.withOutput(new IntWritable(12892), new IntWritable(1));
        driver.runTest();
    }

    /**
     * Test GatherFlightsReduce. This counts the number of flights by airport.
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
     * Test GatherFlightsMap and GatherFlightsReduce.
     */
    @Test
    public void testGatherFlightsMapReduce() throws IOException {
        final PopularAirportsDriver.GatherFlightsMap mapper = new PopularAirportsDriver.GatherFlightsMap();
        final PopularAirportsDriver.GatherFlightsReduce reducer = new PopularAirportsDriver.GatherFlightsReduce();
        final MapReduceDriver<LongWritable, Text, IntWritable, IntWritable, IntWritable, IntWritable> driver = new MapReduceDriver<>(
                mapper, reducer);
        driver.withInput(new LongWritable(0), new Text(
                "2015,1,1,4,2015-01-01,19805,\"N787AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-5.00,\"1230\",7.00,0.00,0.00,"));
        driver.withInput(new LongWritable(1), new Text(
                "2015,1,2,5,2015-01-02,19805,\"N795AA\",\"1\",12478,1247802,31703,12892,1289203,32575,\"0900\",-10.00,\"1230\",-19.00,0.00,0.00,"));
        driver.withInput(new LongWritable(2), new Text(
                "2015,1,9,5,2015-01-09,19805,\"N379AA\",\"5\",11298,1129803,30194,12173,1217302,32134,\"1315\",5.00,\"1758\",-13.00,0.00,0.00,"));
        driver.withOutput(new IntWritable(11298), new IntWritable(1));
        driver.withOutput(new IntWritable(12173), new IntWritable(1));
        driver.withOutput(new IntWritable(12478), new IntWritable(2));
        driver.withOutput(new IntWritable(12892), new IntWritable(2));
        driver.runTest();
    }

    /**
     * Test TopAirportsMap. It does nothing but combine the key and value into a
     * new tuple.
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
    public void testTopAirportsReduce() throws IOException, URISyntaxException {
        final ReduceDriver<NullWritable, AirportFlightsWritable, IntWritable, Text> driver = new ReduceDriver<>();
        final URI uri = this.getClass().getClassLoader().getResource("rita-static.zip").toURI();
        driver.setCacheArchives(new URI[] { uri });
        
        driver.withReducer(new PopularAirportsDriver.TopAirportsReduce());
        driver.withInput(NullWritable.get(), Arrays.asList(new AirportFlightsWritable(12478, 3)));
        driver.withOutput(new IntWritable(3), new Text("John F. Kennedy International"));
        driver.runTest();
    }

    /**
     * Test TopAirportsMap and TopAirportsReduce.
     */
    @Test
    public void testTopAirportsMapReduce() throws IOException, URISyntaxException {
        final PopularAirportsDriver.TopAirportsMap mapper = new PopularAirportsDriver.TopAirportsMap();
        final PopularAirportsDriver.TopAirportsReduce reducer = new PopularAirportsDriver.TopAirportsReduce();
        final MapReduceDriver<Text, Text, NullWritable, AirportFlightsWritable, IntWritable, Text> driver = new MapReduceDriver<>(
                mapper, reducer);
        final URI uri = this.getClass().getClassLoader().getResource("rita-static.zip").toURI();
        driver.setCacheArchives(new URI[] { uri });

        driver.withInput(new Text("12478"), new Text("3"));
        driver.withOutput(new IntWritable(3), new Text("John F. Kennedy International"));
        driver.runTest();
    }
}
