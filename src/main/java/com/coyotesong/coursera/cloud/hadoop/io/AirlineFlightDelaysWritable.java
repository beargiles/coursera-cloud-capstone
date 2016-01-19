package com.coyotesong.coursera.cloud.hadoop.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

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
public class AirlineFlightDelaysWritable extends ArrayWritable implements Comparable<AirlineFlightDelaysWritable> {
    public AirlineFlightDelaysWritable() {
        super(IntWritable.class);
        IntWritable[] ints = new IntWritable[5];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = new IntWritable(0);
        }
        set(ints);
    }

    public AirlineFlightDelaysWritable(int airlineId) {
        super(IntWritable.class);
        IntWritable[] ints = new IntWritable[5];
        ints[0] = new IntWritable(airlineId);
        ints[1] = new IntWritable(0);
        ints[2] = new IntWritable(0);
        ints[3] = new IntWritable(0);
        ints[4] = new IntWritable(0);
        set(ints);
    }

    public AirlineFlightDelaysWritable(int airlineId, int delay) {
        super(IntWritable.class);
        IntWritable[] ints = new IntWritable[5];
        ints[0] = new IntWritable(airlineId);
        ints[1] = new IntWritable(1);
        ints[2] = new IntWritable(delay);
        ints[3] = new IntWritable(delay * delay);
        ints[4] = new IntWritable(delay);
        set(ints);
    }

    public AirlineFlightDelaysWritable(int airlineId, int[] values) {
        super(IntWritable.class);
        IntWritable[] ints = new IntWritable[5];
        ints[0] = new IntWritable(airlineId);
        ints[1] = new IntWritable(values[0]);
        ints[2] = new IntWritable(values[1]);
        ints[3] = new IntWritable(values[2]);
        ints[4] = new IntWritable(values[3]);
        set(ints);
    }

    public int getAirlineId() {
        return ((IntWritable) get()[0]).get();
    }

    public int getNumFlights() {
        return ((IntWritable) get()[1]).get();
    }

    public int getDelay() {
        return ((IntWritable) get()[2]).get();
    }

    public int getDelaySquared() {
        return ((IntWritable) get()[3]).get();
    }

    public int getMaxDelay() {
        return ((IntWritable) get()[4]).get();
    }

    /**
     * Get average delay.
     * 
     * @return
     */
    public double getMean() {
        int flights = getNumFlights();
        double mean = getDelay();
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
        int flights = getNumFlights();
        double stddev = 0;
        if (flights > 1) {
            double x = (double) getDelay();
            double x2 = (double) getDelaySquared();
            stddev = Math.sqrt((x2 - (x * x / flights)) / (flights - 1));
        }
        return stddev;
    }

    /**
     * Add records. We only do this if the delay is greater than zero - we do
     * not consider early arrivals in our statistics.
     * 
     * @param x
     */
    public void add(AirlineFlightDelaysWritable x) {
        if (getAirlineId() != x.getAirlineId()) {
            throw new IllegalArgumentException("airlineIds do not match!");
        }

        if (x.getDelay() > 0) {
            IntWritable ints[] = new IntWritable[5];
            ints[0] = new IntWritable(getAirlineId());
            ints[1] = new IntWritable(getNumFlights() + x.getNumFlights());
            ints[2] = new IntWritable(getDelay() + x.getDelay());
            ints[3] = new IntWritable(getDelaySquared() + x.getDelaySquared());
            ints[4] = new IntWritable(Math.max(getMaxDelay(), x.getMaxDelay()));
            set(ints);
        }
    }

    @Override
    public int hashCode() {
        return getAirlineId();
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
        if (getAirlineId() != other.getAirlineId())
            return false;
        if (getNumFlights() != other.getNumFlights())
            return false;
        if (getDelay() != other.getDelay())
            return false;
        if (getDelaySquared() != other.getDelaySquared())
            return false;
        if (getMaxDelay() != other.getMaxDelay())
            return false;
        return true;
    }

    @Override
    public String toString() {
        return String.format("AirlineFlightDelays %d, %d, %d, %d, %d", getAirlineId(), getNumFlights(), getDelay(),
                getDelaySquared(), getMaxDelay());
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