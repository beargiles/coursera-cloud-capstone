package com.coyotesong.coursera.cloud.domain;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.commons.csv.CSVRecord;

/**
 * Static information about airports.
 * 
 * @author bgiles
 */
@Entity
public class AirportInfo {
    private Integer id;
    private Integer airportId;
    private String airport;
    private String displayAirportName;
    private String displayAirportCityName;
    private Integer airportWac;
    private String countryName;
    private String countryIso;
    private String stateName;
    private String stateCode;
    private Integer stateFips;
    private Integer cityMarketId;
    private String displayCityMarketName;
    private String cityNameFull;
    private Integer cityMarketWac;
    private Date airportStartDate;
    private Date airportThruDate;
    private boolean isClosed;
    private boolean isLatest;

    public AirportInfo() {

    }

    @Id
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getAirportId() {
        return airportId;
    }

    public void setAirportId(Integer airportId) {
        this.airportId = airportId;
    }

    public String getAirport() {
        return airport;
    }

    public void setAirport(String airport) {
        this.airport = airport;
    }

    public String getDisplayAirportName() {
        return displayAirportName;
    }

    public void setDisplayAirportName(String displayAirportName) {
        this.displayAirportName = displayAirportName;
    }

    public String getDisplayAirportCityName() {
        return displayAirportCityName;
    }

    public void setDisplayAirportCityName(String displayAirportCityName) {
        this.displayAirportCityName = displayAirportCityName;
    }

    public Integer getAirportWac() {
        return airportWac;
    }

    public void setAirportWac(Integer airportWac) {
        this.airportWac = airportWac;
    }

    public String getCountryName() {
        return countryName;
    }

    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    public String getCountryIso() {
        return countryIso;
    }

    public void setCountryIso(String countryIso) {
        this.countryIso = countryIso;
    }

    public String getStateName() {
        return stateName;
    }

    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    public String getStateCode() {
        return stateCode;
    }

    public void setStateCode(String stateCode) {
        this.stateCode = stateCode;
    }

    public Integer getStateFips() {
        return stateFips;
    }

    public void setStateFips(Integer stateFips) {
        this.stateFips = stateFips;
    }

    public Integer getCityMarketId() {
        return cityMarketId;
    }

    public void setCityMarketId(Integer cityMarketId) {
        this.cityMarketId = cityMarketId;
    }

    public String getDisplayCityMarketName() {
        return displayCityMarketName;
    }

    public void setDisplayCityMarketName(String displayCityMarketName) {
        this.displayCityMarketName = displayCityMarketName;
    }

    public String getCityNameFull() {
        return cityNameFull;
    }

    public void setCityNameFull(String cityNameFull) {
        this.cityNameFull = cityNameFull;
    }

    public Integer getCityMarketWac() {
        return cityMarketWac;
    }

    public void setCityMarketWac(Integer cityMarketWac) {
        this.cityMarketWac = cityMarketWac;
    }

    @Temporal(TemporalType.DATE)
    public Date getAirportStartDate() {
        return airportStartDate;
    }

    public void setAirportStartDate(Date airportStartDate) {
        this.airportStartDate = airportStartDate;
    }

    @Temporal(TemporalType.DATE)
    public Date getAirportThruDate() {
        return airportThruDate;
    }

    public void setAirportThruDate(Date airportThruDate) {
        this.airportThruDate = airportThruDate;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void setClosed(boolean isClosed) {
        this.isClosed = isClosed;
    }

    public boolean isLatest() {
        return isLatest;
    }

    public void setLatest(boolean isLatest) {
        this.isLatest = isLatest;
    }

    /**
     * Builder.
     * 
     * @author bgiles
     */
    public static class Builder {
        private AirportInfo info = new AirportInfo();

        public Builder(Integer id) {
            info.id = id;
        }

        public void setAirportId(Integer airportId) {
            info.airportId = airportId;
        }

        public void setAirport(String airport) {
            info.airport = airport;
        }

        public void setDisplayAirportName(String displayAirportName) {
            info.displayAirportName = displayAirportName;
        }

        public void setDisplayAirportCityName(String displayAirportCityName) {
            info.displayAirportCityName = displayAirportCityName;
        }

        public void setAirportWac(Integer airportWac) {
            info.airportWac = airportWac;
        }

        public void setCountryName(String countryName) {
            info.countryName = countryName;
        }

        public void setCountryIso(String countryIso) {
            info.countryIso = countryIso;
        }

        public void setStateName(String stateName) {
            info.stateName = stateName;
        }

        public void setStateCode(String stateCode) {
            info.stateCode = stateCode;
        }

        public void setStateFips(Integer stateFips) {
            info.stateFips = stateFips;
        }

        public void setCityMarketId(Integer cityMarketId) {
            info.cityMarketId = cityMarketId;
        }

        public void setDisplayCityMarketName(String displayCityMarketName) {
            info.displayCityMarketName = displayCityMarketName;
        }

        public void setCityNameFull(String cityNameFull) {
            info.cityNameFull = cityNameFull;
        }

        public void setCityMarketWac(Integer cityMarketWac) {
            info.cityMarketWac = cityMarketWac;
        }

        public void setAirportStartDate(Date airportStartDate) {
            info.airportStartDate = airportStartDate;
        }

        public void setAirportThruDate(Date airportThruDate) {
            info.airportThruDate = airportThruDate;
        }

        public void setClosed(boolean isClosed) {
            info.isClosed = isClosed;
        }

        public void setLatest(boolean isLatest) {
            info.isLatest = isLatest;
        }

        public AirportInfo build() {
            return info;
        }
    }

    /**
     * Builder
     * 
     * @author bgiles
     */
    public static class CSV {
        public static AirportInfo parse(CSVRecord record) {
            if (!record.get(0).matches("[0-9]+")) {
                throw new IllegalArgumentException("not valid CSV record");
            }

            final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            final AirportInfo.Builder builder = new AirportInfo.Builder(Integer.valueOf(record.get(0)));

            builder.setAirportId(Integer.valueOf(record.get(1)));
            builder.setAirport(record.get(2));
            builder.setDisplayAirportName(record.get(3));
            builder.setDisplayAirportCityName(record.get(4));
            builder.setAirportWac(Integer.valueOf(record.get(5)));
            builder.setCountryName(record.get(6));
            builder.setCountryIso(record.get(7));
            builder.setStateName(record.get(8));
            builder.setStateCode(record.get(9));
            if (!record.get(10).isEmpty()) {
                builder.setStateFips(Integer.valueOf(record.get(10)));
            }
            if (!record.get(11).isEmpty()) {
                builder.setCityMarketId(Integer.valueOf(record.get(11)));
            }
            builder.setDisplayCityMarketName(record.get(12));
            builder.setCityNameFull(record.get(13));
            if (!record.get(14).isEmpty()) {
                builder.setCityMarketWac(Integer.valueOf(record.get(14)));
            }

            // skip lat, long
            try {
                if (record.get(24).matches("[0-9]{4}-[0-1][0-9]-[0-3][0-9]")) {
                    builder.setAirportStartDate(df.parse(record.get(24)));
                }
                if (record.get(25).matches("[0-9]{4}-[0-1][0-9]-[0-3][0-9]")) {
                    builder.setAirportThruDate(df.parse(record.get(25)));
                }
            } catch (ParseException e) {
                // eat it - not important.
            }

            builder.setClosed("1".equals(record.get(26)));
            builder.setLatest("1".equals(record.get(27)));

            AirportInfo airport = builder.build();
            return airport;
        }
    }
}