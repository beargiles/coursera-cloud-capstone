package com.coyotesong.coursera.cloud.hadoop.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Writable containing (airportId, flights) tuples.
 * 
 * @author bgiles
 */
public class AirportFlightsWritable extends ArrayWritable {
    public AirportFlightsWritable() {
        super(IntWritable.class);
    }

    public AirportFlightsWritable(int airportId, int flights) {
        super(IntWritable.class);
        IntWritable[] ints = new IntWritable[2];
        ints[0] = new IntWritable(airportId);
        ints[1] = new IntWritable(flights);
        set(ints);
    }    
    
    public int getAirportId() {
        return ((IntWritable) get()[0]).get();
    }
    
    public int getFlights() {
        return ((IntWritable) get()[1]).get();
    }
}