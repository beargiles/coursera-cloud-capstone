package com.coyotesong.coursera.cloud.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.coyotesong.coursera.cloud.domain.AirlineFlightDelays;

/**
 * Writable containing (airlineId, flight delay statistics) tuples. This class
 * maintains enough information to calculate basic statistics about the airline
 * performance.
 * 
 * We are specifically concerned about the 95th percentile of the arrival delay
 * time. That is, we want to know the maximum delay on 19 out of 20 flights. The
 * final 1-in-20 delays are probably due to exceptional circumstances and we
 * don't want to unduly penalize the airline.
 * 
 * The comparator compares two objects on the basis of their mean delay + two
 * standard deviations. This corresponds to the 95th percentile of the delay. It
 * does NOT implement WritableComparable since the comparison is not on the
 * 'key'.
 * 
 * @author bgiles
 */
public class AirlineFlightDelaysWritable implements Writable, Comparable<AirlineFlightDelaysWritable> {
    private AirlineFlightDelays delays;
    
    public AirlineFlightDelaysWritable() {
        delays = new AirlineFlightDelays();
    }

    public AirlineFlightDelaysWritable(int airlineId) {
        delays = new AirlineFlightDelays(airlineId);
    }

    public AirlineFlightDelaysWritable(int airlineId, int delay) {
        delays = new AirlineFlightDelays(airlineId, delay);
    }

    public AirlineFlightDelaysWritable(int airlineId, int[] values) {
        delays = new AirlineFlightDelays(airlineId, values[0], values[1], values[2], values[3]);
    }

    public AirlineFlightDelays getAirlineFlightDelays() {
        return delays;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(delays.getAirlineId());
        out.writeInt(delays.getNumFlights());
        out.writeInt(delays.getDelay());
        out.writeInt(delays.getDelaySquared());
        out.writeInt(delays.getMaxDelay());
        out.writeInt(delays.getMiles());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        delays.setAirlineId(in.readInt());
        delays.setNumFlights(in.readInt());
        delays.setDelay(in.readInt());
        delays.setDelaySquared(in.readInt());
        delays.setMaxDelay(in.readInt());
        delays.setMiles(in.readInt());
    }

    /**
     * Add records.
     * 
     * @param x
     */
    public void add(AirlineFlightDelaysWritable x) {
        delays.add(x.getAirlineFlightDelays());
    }

    @Override
    public int hashCode() {
        return delays.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AirlineFlightDelaysWritable other = (AirlineFlightDelaysWritable) obj;
        return delays.equals(other.getAirlineFlightDelays());
    }

    @Override
    public String toString() {
        return delays.toString();
    }

    /**
     * Compare records according to two standard deviations above mean.
     */
    @Override
    public int compareTo(AirlineFlightDelaysWritable w) {
        return delays.compareTo(w.getAirlineFlightDelays());
    }
}