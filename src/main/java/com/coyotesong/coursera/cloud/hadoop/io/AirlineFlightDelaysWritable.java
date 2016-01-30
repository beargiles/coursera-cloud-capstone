package com.coyotesong.coursera.cloud.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

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
    private int airlineId;
    private int numFlights;
    private int delay;
    private int delaySquared;
    private int maxDelay;
    private int miles;
    
    public AirlineFlightDelaysWritable() {
    }

    public AirlineFlightDelaysWritable(int airlineId) {
        this.airlineId = airlineId;
    }

    public AirlineFlightDelaysWritable(int airlineId, int delay) {
        this.airlineId = airlineId;
        this.numFlights = 1;
        this.delay = delay;
        this.delaySquared = delay * delay;
        this.maxDelay = delay;
    }

    public AirlineFlightDelaysWritable(int airlineId, int[] values) {
        this.airlineId = airlineId;
        this.numFlights = values[0];
        this.delay = values[1];
        this.delaySquared = values[2];
        this.maxDelay = values[3];
    }

    public int getAirlineId() {
        return airlineId;
    }

    public int getNumFlights() {
        return numFlights;
    }

    public int getDelay() {
        return delay;
    }

    public int getDelaySquared() {
        return delaySquared;
    }

    public int getMaxDelay() {
        return maxDelay;
    }

    /**
     * Get average delay.
     * 
     * @return
     */
    public double getMean() {
        int flights = numFlights;
        double mean = delay;
        if (flights > 1) {
            mean /= flights;
        }

        return mean;
    }

    /**
     * Get standard deviation of delays
     * 
     * @return
     */
    public double getStdDev() {
        int flights = numFlights;
        double stddev = 0;
        if (flights > 1) {
            double x = (double) delay;
            double x2 = (double) delaySquared;
            stddev = Math.sqrt((x2 - (x * x / flights)) / (flights - 1));
        }
        return stddev;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(airlineId);
        out.writeInt(numFlights);
        out.writeInt(delay);
        out.writeInt(delaySquared);
        out.writeInt(maxDelay);
        out.writeInt(miles);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airlineId = in.readInt();
        numFlights = in.readInt();
        delay = in.readInt();
        delaySquared = in.readInt();
        maxDelay = in.readInt();
        miles = in.readInt();
    }

    /**
     * Add records.
     * 
     * @param x
     */
    public void add(AirlineFlightDelaysWritable x) {
        if (airlineId != x.airlineId) {
            throw new IllegalArgumentException("airlineIds do not match!");
        }

        numFlights += x.numFlights;
        delay += x.delay;
        delaySquared += x.delaySquared;
        maxDelay = Math.max(maxDelay, x.maxDelay);
        miles += x.miles;
    }

    @Override
    public int hashCode() {
        return airlineId;
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
        if (airlineId != other.airlineId)
            return false;
        if (numFlights != other.numFlights)
            return false;
        if (delay != other.delay)
            return false;
        if (delaySquared != other.delaySquared)
            return false;
        if (maxDelay != other.maxDelay)
            return false;
        if (miles != other.miles)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return String.format("%d,%d,%d,%d,%d", airlineId, numFlights, delay,
                delaySquared, maxDelay);
    }

    /**
     * Compare records according to two standard deviations above mean.
     */
    @Override
    public int compareTo(AirlineFlightDelaysWritable w) {
        double lhs = getMean() + 2 * getStdDev();
        double rhs = w.getMean() + 2 * w.getStdDev();
        if (lhs < rhs) {
            return -1;
        } else if (lhs == rhs) {
            return 0;
        }
        return 1;
    }
}