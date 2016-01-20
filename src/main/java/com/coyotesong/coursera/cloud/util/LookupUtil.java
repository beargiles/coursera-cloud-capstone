package com.coyotesong.coursera.cloud.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.coyotesong.coursera.cloud.domain.LookupAirline;
import com.coyotesong.coursera.cloud.domain.LookupAirport;

/**
 * Utility class for lookup classes
 * 
 * @author bgiles
 */
public class LookupUtil {
    public static final Map<Integer, LookupAirline> AIRLINES = new HashMap<>();
    public static final Map<Integer, LookupAirport> AIRPORTS = new HashMap<>();

    /**
     * Load contents from specified zip file.
     * 
     * @param file
     * @throws IOException
     */
    public static void load(File file) throws IOException {
        AIRLINES.clear();
        AIRPORTS.clear();
        try (ZipFile zf = new ZipFile(file)) {
            ZipEntry ze = zf.getEntry("L_AIRLINE_ID.csv");
            if (ze != null) {
                try (InputStream is = zf.getInputStream(ze)) {
                    loadAirlines(is);
                }
            }

            ze = zf.getEntry("L_AIRPORT_ID.csv");
            if (ze != null) {
                try (InputStream is = zf.getInputStream(ze)) {
                    loadAirports(is);
                }
            }
        }
    }

    /**
     * Load airline information from csv file.
     * 
     * @param is
     * @throws IOException
     */
    public static void loadAirlines(InputStream is) throws IOException {
        try (Reader r = new InputStreamReader(is); LineNumberReader lnr = new LineNumberReader(r)) {
            String s = null;
            while ((s = lnr.readLine()) != null) {
                if (lnr.getLineNumber() == 1) {
                    continue;
                }
                final List<String> fields = CSVParser.parse(s);
                final String[] subfields = fields.get(1).split(":");
                final Integer code = Integer.valueOf(fields.get(0));
                final String name = subfields[0].trim();
                final String abbreviation = subfields[1].trim();
                final LookupAirline airline = new LookupAirline(code, name, abbreviation);
                AIRLINES.put(airline.getCode(), airline);
            }
        }
    }

    /**
     * Load airport information from csv file.
     * 
     * @param is
     * @throws IOException
     */
    public static void loadAirports(InputStream is) throws IOException {
        try (Reader r = new InputStreamReader(is); LineNumberReader lnr = new LineNumberReader(r)) {
            String s = null;
            while ((s = lnr.readLine()) != null) {
                if (lnr.getLineNumber() == 1) {
                    continue;
                }
                final List<String> fields = CSVParser.parse(s);
                final String[] subfields = fields.get(1).split(":");
                final Integer code = Integer.valueOf(fields.get(0));
                final String location = subfields[0].trim();
                final String name = (code == 99999) ? "Unknown Point" : subfields[1].trim();
                final LookupAirport airport = new LookupAirport(code, name, location);
                AIRPORTS.put(airport.getCode(), airport);
            }
        }
    }
}
