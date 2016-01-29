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
 * Static information airlines/carriers.
 * 
 * @author bgiles
 */
@Entity
public class AirlineInfo {
    private Integer airlineId;
    private String carrier;
    private String carrierEntity;
    private String carrierName;
    private String uniqueCarrierCode;
    private String uniqueCarrierEntity;
    private String uniqueCarrierName;
    private int wac;
    private int carrierGroup;
    private int carrierGroupNew;
    private String region;
    private Date startDate;
    private Date stopDate;

    public AirlineInfo() {

    }

    @Id
    public Integer getAirlineId() {
        return airlineId;
    }

    public void setAirlineId(Integer airlineId) {
        this.airlineId = airlineId;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getCarrierEntity() {
        return carrierEntity;
    }

    public void setCarrierEntity(String carrierEntity) {
        this.carrierEntity = carrierEntity;
    }

    public String getCarrierName() {
        return carrierName;
    }

    public void setCarrierName(String carrierName) {
        this.carrierName = carrierName;
    }

    public String getUniqueCarrierCode() {
        return uniqueCarrierCode;
    }

    public void setUniqueCarrierCode(String uniqueCarrierCode) {
        this.uniqueCarrierCode = uniqueCarrierCode;
    }

    public String getUniqueCarrierEntity() {
        return uniqueCarrierEntity;
    }

    public void setUniqueCarrierEntity(String uniqueCarrierEntity) {
        this.uniqueCarrierEntity = uniqueCarrierEntity;
    }

    public String getUniqueCarrierName() {
        return uniqueCarrierName;
    }

    public void setUniqueCarrierName(String uniqueCarrierName) {
        this.uniqueCarrierName = uniqueCarrierName;
    }

    public int getWac() {
        return wac;
    }

    public void setWac(int wac) {
        this.wac = wac;
    }

    public int getCarrierGroup() {
        return carrierGroup;
    }

    public void setCarrierGroup(int carrierGroup) {
        this.carrierGroup = carrierGroup;
    }

    public int getCarrierGroupNew() {
        return carrierGroupNew;
    }

    public void setCarrierGroupNew(int carrierGroupNew) {
        this.carrierGroupNew = carrierGroupNew;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Temporal(TemporalType.DATE)
    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    @Temporal(TemporalType.DATE)
    public Date getStopDate() {
        return stopDate;
    }

    public void setStopDate(Date stopDate) {
        this.stopDate = stopDate;
    }

    /**
     * Builder
     * 
     * @author bgiles
     */
    public static class Builder {
        private AirlineInfo info = new AirlineInfo();

        public Builder(int id) {
            info.setAirlineId(id);
        }

        public void setCarrier(String carrier) {
            info.carrier = carrier;
        }

        public void setCarrierEntity(String carrierEntity) {
            info.carrierEntity = carrierEntity;
        }

        public void setCarrierName(String carrierName) {
            info.carrierName = carrierName;
        }

        public void setUniqueCarrierCode(String uniqueCarrierCode) {
            info.uniqueCarrierCode = uniqueCarrierCode;
        }

        public void setUniqueCarrierEntity(String uniqueCarrierEntity) {
            info.uniqueCarrierEntity = uniqueCarrierEntity;
        }

        public void setUniqueCarrierName(String uniqueCarrierName) {
            info.uniqueCarrierName = uniqueCarrierName;
        }

        public void setWac(int wac) {
            info.wac = wac;
        }

        public void setCarrierGroup(int carrierGroup) {
            info.carrierGroup = carrierGroup;
        }

        public void setCarrierGroupNew(int carrierGroupNew) {
            info.carrierGroupNew = carrierGroupNew;
        }

        public void setRegion(String region) {
            info.region = region;
        }

        public void setStartDate(Date startDate) {
            info.startDate = startDate;
        }

        public void setStopDate(Date stopDate) {
            info.stopDate = stopDate;
        }

        public AirlineInfo build() {
            return info;
        }
    }

    /**
     * Builder
     *
     * @author bgiles
     */
    public static class CSV {
        public static AirlineInfo parse(CSVRecord record) {
            if (!record.get(0).matches("[0-9]+")) {
                throw new IllegalArgumentException("not valid CSV record");
            }

            final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            final Builder builder = new Builder(Integer.parseInt(record.get(0)));

            builder.setCarrier(record.get(1));
            builder.setCarrierEntity(record.get(2));
            builder.setCarrierName(record.get(3));
            builder.setUniqueCarrierCode(record.get(4));
            builder.setUniqueCarrierEntity(record.get(5));
            builder.setUniqueCarrierName(record.get(6));
            builder.setWac(Integer.valueOf(record.get(7)));
            builder.setCarrierGroup(Integer.valueOf(record.get(8)));
            builder.setCarrierGroupNew(Integer.valueOf(record.get(9)));
            builder.setRegion(record.get(10));

            try {
                if (record.get(11).matches("[0-9]{4}-[0-1][0-9]-[0-3][0-9]")) {
                    builder.setStartDate(df.parse(record.get(11)));
                }
                if (record.get(12).matches("[0-9]{4}-[0-1][0-9]-[0-3][0-9]")) {
                    builder.setStopDate(df.parse(record.get(12)));
                }
            } catch (ParseException e) {
                // eat it -- not important
            }

            return builder.build();
        }
    }

}