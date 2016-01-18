package com.coyotesong.coursera.cloud.domain;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * Static information airlines/carriers.
 * 
 * @author bgiles
 */
@Entity
public class AirlineInfo {
    private Integer id;
    private Integer airlineId;
    private String name;
    
    public AirlineInfo() {
        
    }

    public AirlineInfo(Integer airlineId, String name) {
        this.airlineId = airlineId;
        this.name = name;
    }

    @Id
    @GeneratedValue(strategy=GenerationType.AUTO)
    public Integer getId() {
        return id;
    }
    
    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getAirlineId() {
        return airlineId;
    }

    public void setAirlineId(Integer airlineId) {
        this.airlineId = airlineId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}