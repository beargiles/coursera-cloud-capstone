package com.coyotesong.coursera.cloud.hadoop.io;

/**
 * Sorting comparator that considers airports and airline id. 
 * 
 * @author bgiles
 */
public class AirportsAndAirlineSortingComparator
        extends AirportsAndAirlineGroupingComparator {

    /**
     * Compare two objects. We add flightId to sort criteria.
     */
    @Override
    public int compare(Object lobj, Object robj) {
        // perform grouping comparison
        int s = super.compare(lobj, robj);
        if (s != 0) {
            return s;
        }

        // now compare airline ids
        AirportsAndAirlineWritable l = (AirportsAndAirlineWritable) lobj;
        AirportsAndAirlineWritable r = (AirportsAndAirlineWritable) robj;

        int lhs = l.getAirlineId();
        int rhs = r.getAirlineId();
        if (lhs < rhs) {
            return -1;
        } else if (lhs > rhs) {
            return 1;
        }
        return 0;
    }
}
