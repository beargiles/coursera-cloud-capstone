package com.coyotesong.coursera.cloud.hadoop.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Grouping comparator that only considers origin
 * and destination airports. 
 * 
 * @author bgiles
 */
public class AirportsAndAirlineGroupingComparator
        extends WritableComparator {
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int p = readInt(b1, s1);
        int q = readInt(b2, s2);
        if (p < q) {
            return -1;
        } else if (p > q) {
            return 1;
        }
        
        p = readInt(b1, s1 + 4);
        q = readInt(b2, s2 + 4);

        if (p < q) {
            return -1;
        } else if (p > q) {
            return 1;
        }
        
        return 0;
    }
}
