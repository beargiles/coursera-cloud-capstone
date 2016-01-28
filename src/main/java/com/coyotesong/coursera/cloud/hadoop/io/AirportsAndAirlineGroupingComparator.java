package com.coyotesong.coursera.cloud.hadoop.io;

import org.apache.hadoop.io.WritableComparator;

/**
 * Grouping comparator that only considers origin
 * and destination airports. 
 * 
 * @author bgiles
 */
public class AirportsAndAirlineGroupingComparator
        extends WritableComparator {

    /**
     * Compare two objects. We only compare the airports.
     */
    @Override
    public int compare(Object lobj, Object robj) {
        AirportsAndAirlineWritable l = (AirportsAndAirlineWritable) lobj;
        AirportsAndAirlineWritable r = (AirportsAndAirlineWritable) robj;

        int lhs = l.getOriginId();
        int rhs = r.getOriginId();
        if (lhs < rhs) {
            return -1;
        } else if (lhs > rhs) {
            return 1;
        }

        lhs = l.getDestinationId();
        rhs = r.getDestinationId();
        if (lhs < rhs) {
            return -1;
        } else if (lhs > rhs) {
            return 1;
        }
        return 0;
    }
}
