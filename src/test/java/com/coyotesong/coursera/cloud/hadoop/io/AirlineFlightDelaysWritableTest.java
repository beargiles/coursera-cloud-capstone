package com.coyotesong.coursera.cloud.hadoop.io;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Test AirlineFlightDelaysWritable.
 * 
 * TODO test add()
 * TODO test calculation of mean and standard deviation
 * 
 * @author bgiles
 */
public class AirlineFlightDelaysWritableTest {

    @Test
    public void testDefaultConstructor() {
        final AirlineFlightDelaysWritable actual = new AirlineFlightDelaysWritable();
        assertEquals(0, actual.getAirlineId());
        assertEquals(0, actual.getNumFlights());
        assertEquals(0, actual.getDelay());
        assertEquals(0, actual.getDelaySquared());
        assertEquals(0, actual.getMaxDelay());
        assertEquals(0.0, actual.getMean(), 0.0001);
        assertEquals(0.0, actual.getStdDev(), 0.0001);
    }

    @Test
    public void testConstructor() {
        final int airlineId = 1;
        final int delay = 2;
        final AirlineFlightDelaysWritable actual = new AirlineFlightDelaysWritable(airlineId, delay);
        assertEquals(airlineId, actual.getAirlineId());
        assertEquals(1, actual.getNumFlights());
        assertEquals(2, actual.getDelay());
        assertEquals(4, actual.getDelaySquared());
        assertEquals(2, actual.getMaxDelay());
        assertEquals(2.0, actual.getMean(), 0.0001);
        assertEquals(0.0, actual.getStdDev(), 0.0001);
    }

    @Test
    public void testConstructorInts() {
        final int airlineId = 1;
        final int[] values = new int[] { 2, -3, 4, 5 };
        final AirlineFlightDelaysWritable actual = new AirlineFlightDelaysWritable(airlineId, values);
        assertEquals(airlineId, actual.getAirlineId());
        assertEquals(values[0], actual.getNumFlights());
        assertEquals(values[1], actual.getDelay());
        assertEquals(values[2], actual.getDelaySquared());
        assertEquals(values[3], actual.getMaxDelay());
    }

    @Test
    public void testDataStream() throws IOException {
        final int airlineId = 1;
        final int delay = 2;
        final AirlineFlightDelaysWritable w = new AirlineFlightDelaysWritable(airlineId, delay);

        byte[] data = null;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            w.write(out);
            data = baos.toByteArray();
        }

        final AirlineFlightDelaysWritable actual = new AirlineFlightDelaysWritable();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                DataInputStream in = new DataInputStream(bais)) {

            actual.readFields(in);
        }

        assertEquals(airlineId, actual.getAirlineId());
        assertEquals(1, actual.getNumFlights());
        assertEquals(2, actual.getDelay());
        assertEquals(4, actual.getDelaySquared());
        assertEquals(2, actual.getMaxDelay());
        assertEquals(2.0, actual.getMean(), 0.0001);
        assertEquals(0.0, actual.getStdDev(), 0.0001);
    }
}
