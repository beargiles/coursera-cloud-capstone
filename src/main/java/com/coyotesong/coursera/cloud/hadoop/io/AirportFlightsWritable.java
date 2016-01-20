package com.coyotesong.coursera.cloud.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Writable containing (airportId, flights) tuples.
 * 
 * @author bgiles
 */
public class AirportFlightsWritable implements Writable {
    private int airportId;
    private int flights;
    
    public AirportFlightsWritable() {
    }

    public AirportFlightsWritable(int airportId, int flights) {
        this.airportId = airportId;
        this.flights = flights;
    }    
    
    public int getAirportId() {
        return airportId;
    }
    
    public int getFlights() {
        return flights;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(airportId);
        out.writeInt(flights);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airportId = in.readInt();
        flights = in.readInt();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + airportId;
        result = prime * result + flights;
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AirportFlightsWritable other = (AirportFlightsWritable) obj;
        if (airportId != other.airportId)
            return false;
        if (flights != other.flights)
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("[AirportFlights %d, %d]", airportId, flights);
    }
}