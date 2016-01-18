package com.coyotesong.coursera.cloud.hadoop.mapreduce;

/**
 * Convenience class that contains a pair of values and can be easily
 * sorted on the first value.
 * 
 * @author bgiles
 *
 * @param <K>
 * @param <V>
 */
class Pair<K extends Comparable<K>, V> implements Comparable<Pair<K,V>> {
    private final K key;
    private final V value;
    
    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public int compareTo(Pair<K, V> p) {
        return key.compareTo(p.key);
    }
}