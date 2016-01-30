package com.coyotesong.coursera.cloud.domain;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.commons.csv.CSVRecord;

/**
 * Raw ontime performance data.
 * 
 * @author bgiles
 */
@Entity
public class OntimeInfo {
    private Long id;
    private Integer year;
    private Integer month;
    private Integer dayOfMonth;
    private Integer dayOfWeek;
    private Date flightDate;
    private Integer airlineId;
    private String tailNumber;
    private String flightNumber;
    private Integer originAirportId;
    private Integer originAirportSeqId;
    private Integer originCityMarketId;
    private Integer destAirportId;
    private Integer destAirportSeqId;
    private Integer destCityMarketId;
    private String crsDepartureTime;
    private Float DepartureDelay;
    private String crsArrivalTime;
    private Float ArrivalDelay;
    private boolean isCancelled;
    private boolean isDiverted;

    public OntimeInfo() {

    }

    @Id
    @GeneratedValue
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public Integer getDayOfMonth() {
        return dayOfMonth;
    }

    public void setDayOfMonth(int dayOfMonth) {
        this.dayOfMonth = dayOfMonth;
    }

    public Integer getDayOfWeek() {
        return dayOfWeek;
    }

    public void setDayOfWeek(int dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    @Temporal(TemporalType.DATE)
    public Date getFlightDate() {
        return flightDate;
    }

    public void setFlightDate(Date flightDate) {
        this.flightDate = flightDate;
    }

    public Integer getAirlineId() {
        return airlineId;
    }

    public void setAirlineId(int airlineId) {
        this.airlineId = airlineId;
    }

    public String getTailNumber() {
        return tailNumber;
    }

    public void setTailNumber(String tailNumber) {
        this.tailNumber = tailNumber;
    }

    public String getFlightNumber() {
        return flightNumber;
    }

    public void setFlightNumber(String flightNumber) {
        this.flightNumber = flightNumber;
    }

    public Integer getOriginAirportId() {
        return originAirportId;
    }

    public void setOriginAirportId(int originAirportId) {
        this.originAirportId = originAirportId;
    }

    public Integer getOriginAirportSeqId() {
        return originAirportSeqId;
    }

    public void setOriginAirportSeqId(int originAirportSeqId) {
        this.originAirportSeqId = originAirportSeqId;
    }

    public Integer getOriginCityMarketId() {
        return originCityMarketId;
    }

    public void setOriginCityMarketId(int originCityMarketId) {
        this.originCityMarketId = originCityMarketId;
    }

    public Integer getDestAirportId() {
        return destAirportId;
    }

    public void setDestAirportId(int destAirportId) {
        this.destAirportId = destAirportId;
    }

    public Integer getDestAirportSeqId() {
        return destAirportSeqId;
    }

    public void setDestAirportSeqId(int destAirportSeqId) {
        this.destAirportSeqId = destAirportSeqId;
    }

    public Integer getDestCityMarketId() {
        return destCityMarketId;
    }

    public void setDestCityMarketId(int destCityMarketId) {
        this.destCityMarketId = destCityMarketId;
    }

    public String getCrsDepartureTime() {
        return crsDepartureTime;
    }

    public void setCrsDepartureTime(String crsDepartureTime) {
        this.crsDepartureTime = crsDepartureTime;
    }

    public Float getDepartureDelay() {
        return DepartureDelay;
    }

    public void setDepartureDelay(Float DepartureDelay) {
        this.DepartureDelay = DepartureDelay;
    }

    public String getCrsArrivalTime() {
        return crsArrivalTime;
    }

    public void setCrsArrivalTime(String crsArrivalTime) {
        this.crsArrivalTime = crsArrivalTime;
    }

    public Float getArrivalDelay() {
        return ArrivalDelay;
    }

    public void setArrivalDelay(Float ArrivalDelay) {
        this.ArrivalDelay = ArrivalDelay;
    }

    public boolean isCancelled() {
        return isCancelled;
    }

    public void setCancelled(boolean isCancelled) {
        this.isCancelled = isCancelled;
    }

    public boolean isDiverted() {
        return isDiverted;
    }

    public void setDiverted(boolean isDiverted) {
        this.isDiverted = isDiverted;
    }

    /**
     * Builder
     * 
     * @author bgiles
     */
    public static class Builder {
        private OntimeInfo info = new OntimeInfo();

        public void setId(Long id) {
            info.id = id;
        }

        public void setYear(int year) {
            info.year = year;
        }

        public void setMonth(int month) {
            info.month = month;
        }

        public void setDayOfMonth(int dayOfMonth) {
            info.dayOfMonth = dayOfMonth;
        }

        public void setDayOfWeek(int dayOfWeek) {
            info.dayOfWeek = dayOfWeek;
        }

        public void setFlightDate(Date flightDate) {
            info.flightDate = flightDate;
        }

        public void setAirlineId(int airlineId) {
            info.airlineId = airlineId;
        }

        public void setTailNumber(String tailNumber) {
            info.tailNumber = tailNumber;
        }

        public void setFlightNumber(String flightNumber) {
            info.flightNumber = flightNumber;
        }

        public void setOriginAirportId(int originAirportId) {
            info.originAirportId = originAirportId;
        }

        public void setOriginAirportSeqId(int originAirportSeqId) {
            info.originAirportSeqId = originAirportSeqId;
        }

        public void setOriginCityMarketId(int originCityMarketId) {
            info.originCityMarketId = originCityMarketId;
        }

        public void setDestAirportId(int destAirportId) {
            info.destAirportId = destAirportId;
        }

        public void setDestAirportSeqId(int destAirportSeqId) {
            info.destAirportSeqId = destAirportSeqId;
        }

        public void setDestCityMarketId(int destCityMarketId) {
            info.destCityMarketId = destCityMarketId;
        }

        public void setCrsDepartureTime(String crsDepartureTime) {
            info.crsDepartureTime = crsDepartureTime;
        }

        public void setDepartureDelay(Float DepartureDelay) {
            info.DepartureDelay = DepartureDelay;
        }

        public void setCrsArrivalTime(String crsArrivalTime) {
            info.crsArrivalTime = crsArrivalTime;
        }

        public void setArrivalDelay(Float ArrivalDelay) {
            info.ArrivalDelay = ArrivalDelay;
        }

        public void setCancelled(boolean isCancelled) {
            info.isCancelled = isCancelled;
        }

        public void setDiverted(boolean isDiverted) {
            info.isDiverted = isDiverted;
        }
        
        public OntimeInfo build() {
            return info;
        }

        public static OntimeInfo build(List<String> values) {
            final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            final Builder builder = new Builder();
            
            builder.setYear(Integer.parseInt(values.get(0)));
            builder.setMonth(Integer.parseInt(values.get(1)));
            builder.setDayOfMonth(Integer.parseInt(values.get(2)));
            builder.setDayOfWeek(Integer.parseInt(values.get(3)));
            try {
                builder.setFlightDate(df.parse(values.get(4)));
            } catch (ParseException e) {
                
            }
            builder.setAirlineId(Integer.parseInt(values.get(5)));
            builder.setTailNumber(values.get(6));
            builder.setFlightNumber(values.get(7));
            builder.setOriginAirportId(Integer.parseInt(values.get(8)));
            builder.setOriginAirportSeqId(Integer.parseInt(values.get(9)));
            builder.setOriginCityMarketId(Integer.parseInt(values.get(10)));
            builder.setDestAirportId(Integer.parseInt(values.get(11)));
            builder.setDestAirportSeqId(Integer.parseInt(values.get(12)));
            builder.setDestCityMarketId(Integer.parseInt(values.get(13)));
            builder.setCrsDepartureTime(values.get(14));
            if (!values.get(15).isEmpty()) {
                builder.setDepartureDelay(Float.parseFloat(values.get(15)));
            }
            if (!values.get(16).isEmpty()) {
                builder.setCrsArrivalTime(values.get(16));
            }
            if (!values.get(17).isEmpty()) {
                builder.setArrivalDelay(Float.parseFloat(values.get(17)));
            }
            builder.setCancelled(!"0.00".equals(values.get(18)));
            builder.setDiverted(!"0.00".equals(values.get(19)));

            return builder.build();
        }
    }

    /**
     * Builder.
     * 
     * @author bgiles
     */
    public static class CSV {
        public static OntimeInfo parse(CSVRecord record) {
            final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            final Builder builder = new Builder();
            
            builder.setYear(Integer.parseInt(record.get(0)));
            builder.setMonth(Integer.parseInt(record.get(1)));
            builder.setDayOfMonth(Integer.parseInt(record.get(2)));
            builder.setDayOfWeek(Integer.parseInt(record.get(3)));
            try {
                builder.setFlightDate(df.parse(record.get(4)));
            } catch (ParseException e) {
                
            }
            builder.setAirlineId(Integer.parseInt(record.get(5)));
            builder.setTailNumber(record.get(6));
            builder.setFlightNumber(record.get(7));
            builder.setOriginAirportId(Integer.parseInt(record.get(8)));
            builder.setOriginAirportSeqId(Integer.parseInt(record.get(9)));
            builder.setOriginCityMarketId(Integer.parseInt(record.get(10)));
            builder.setDestAirportId(Integer.parseInt(record.get(11)));
            builder.setDestAirportSeqId(Integer.parseInt(record.get(12)));
            builder.setDestCityMarketId(Integer.parseInt(record.get(13)));
            builder.setCrsDepartureTime(record.get(14));
            if (!record.get(15).isEmpty()) {
                builder.setDepartureDelay(Float.parseFloat(record.get(15)));
            }
            if (!record.get(16).isEmpty()) {
                builder.setCrsArrivalTime(record.get(16));
            }
            if (!record.get(17).isEmpty()) {
                builder.setArrivalDelay(Float.parseFloat(record.get(17)));
            }
            builder.setCancelled(!"0.00".equals(record.get(18)));
            builder.setDiverted(!"0.00".equals(record.get(19)));

            return builder.build();
        }
    }
}
