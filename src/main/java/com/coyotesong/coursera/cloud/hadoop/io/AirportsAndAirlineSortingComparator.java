package com.coyotesong.coursera.cloud.hadoop.io;

/**
 * Sorting comparator that considers airports and airline id. 
 * 
 * @author bgiles
 */
public class AirportsAndAirlineSortingComparator
        extends AirportsAndAirlineGroupingComparator {
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int r = super.compare(b1, s1, l1, b2, s2, l2);
        if (r != 0) {
            return r;
        }

        int p = readInt(b1, s1 + 8);
        int q = readInt(b2, s2 + 8);
        if (p < q) {
            return -1;
        } else if (p > q) {
            return 1;
        }
        
        return 0;
    }
}
