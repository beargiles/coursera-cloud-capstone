package com.coyotesong.coursera.cloud.hadoop.io;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Test AirportFlightsWritable.
 * 
 * @author bgiles
 */
public class AirportFlightsWritableTest {

    @Test
    public void testDefaultConstructor() {
        final AirportFlightsWritable actual = new AirportFlightsWritable();
        assertEquals(0, actual.getAirportId());
        assertEquals(0, actual.getFlights());
    }

    @Test
    public void testConstructor() {
        final int airportId = 1;
        final int flights = 2;
        final AirportFlightsWritable actual = new AirportFlightsWritable(airportId, flights);
        assertEquals(airportId, actual.getAirportId());
        assertEquals(flights, actual.getFlights());
    }

    @Test
    public void testDataStream() throws IOException {
        final int airportId = 1;
        final int flights = 2;
        final AirportFlightsWritable w = new AirportFlightsWritable(airportId, flights);

        byte[] data = null;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            w.write(out);
            data = baos.toByteArray();
        }

        final AirportFlightsWritable actual = new AirportFlightsWritable();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                DataInputStream in = new DataInputStream(bais)) {

            actual.readFields(in);
        }

        assertEquals(airportId, actual.getAirportId());
        assertEquals(flights, actual.getFlights());
    }
}
