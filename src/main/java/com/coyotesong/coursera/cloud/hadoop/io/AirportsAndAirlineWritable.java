package com.coyotesong.coursera.cloud.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Writable containing a flight's origin, destination, and airline.
 * 
 * @author bgiles
 */
public class AirportsAndAirlineWritable implements Writable {
    private int originId;
    private int destinationId;
    private int airlineId;
   
    public AirportsAndAirlineWritable() {

    }

    public AirportsAndAirlineWritable(int originId, int destinationId, int airlineId) {
        this.originId = originId;
        this.destinationId = destinationId;
        this.airlineId = airlineId;
    }

    public int getOriginId() {
        return originId;
    }

    public int getDestinationId() {
        return destinationId;
    }

    public int getAirlineId() {
        return airlineId;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(originId);
        out.writeInt(destinationId);
        out.writeInt(airlineId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        originId = in.readInt();
        destinationId = in.readInt();
        airlineId = in.readInt();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + destinationId;
        result = prime * result + originId;
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
        AirportsAndAirlineWritable other = (AirportsAndAirlineWritable) obj;
        if (airlineId != other.airlineId)
            return false;
        if (destinationId != other.destinationId)
            return false;
        if (originId != other.originId)
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("%d,%d,%d", originId, destinationId, airlineId);
    }
}
