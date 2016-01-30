package com.coyotesong.coursera.cloud.hadoop.io;

import org.junit.Test;

import com.coyotesong.coursera.cloud.domain.AirlineFlightDelays;

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
        final AirlineFlightDelays actual = new AirlineFlightDelaysWritable().getAirlineFlightDelays();
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
        final AirlineFlightDelays actual = new AirlineFlightDelaysWritable(airlineId, delay).getAirlineFlightDelays();
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
        final AirlineFlightDelays actual = new AirlineFlightDelaysWritable(airlineId, values).getAirlineFlightDelays();
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

        final AirlineFlightDelaysWritable dw = new AirlineFlightDelaysWritable();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                DataInputStream in = new DataInputStream(bais)) {

            dw.readFields(in);
        }

        final AirlineFlightDelays actual = dw.getAirlineFlightDelays();
        assertEquals(airlineId, actual.getAirlineId());
        assertEquals(1, actual.getNumFlights());
        assertEquals(2, actual.getDelay());
        assertEquals(4, actual.getDelaySquared());
        assertEquals(2, actual.getMaxDelay());
        assertEquals(2.0, actual.getMean(), 0.0001);
        assertEquals(0.0, actual.getStdDev(), 0.0001);
    }
}
