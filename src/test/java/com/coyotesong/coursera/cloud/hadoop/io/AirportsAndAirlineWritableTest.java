package com.coyotesong.coursera.cloud.hadoop.io;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Test AirportsAndAirlineWritable.
 * 
 * @author bgiles
 */
public class AirportsAndAirlineWritableTest {

    @Test
    public void testDefaultConstructor() {
        final AirportsAndAirlineWritable actual = new AirportsAndAirlineWritable();
        assertEquals(0, actual.getOriginId());
        assertEquals(0, actual.getDestinationId());
        assertEquals(0, actual.getAirlineId());
    }

    @Test
    public void testConstructor() {
        final int originId = 1;
        final int destinationId = 2;
        final int airlineId = 3;
        final AirportsAndAirlineWritable actual = new AirportsAndAirlineWritable(originId, destinationId, airlineId);
        assertEquals(originId, actual.getOriginId());
        assertEquals(destinationId, actual.getDestinationId());
        assertEquals(airlineId, actual.getAirlineId());
    }

    @Test
    public void testDataStream() throws IOException {
        final int originId = 1;
        final int destinationId = 2;
        final int airlineId = 3;
        final AirportsAndAirlineWritable w = new AirportsAndAirlineWritable(originId, destinationId, airlineId);

        byte[] data = null;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            w.write(out);
            data = baos.toByteArray();
        }

        final AirportsAndAirlineWritable actual = new AirportsAndAirlineWritable();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                DataInputStream in = new DataInputStream(bais)) {

            actual.readFields(in);
        }

        assertEquals(originId, actual.getOriginId());
        assertEquals(destinationId, actual.getDestinationId());
        assertEquals(airlineId, actual.getAirlineId());
    }
}
