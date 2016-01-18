package com.coyotesong.coursera.cloud.domain;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * Static information about airports.
 * 
 * @author bgiles
 */
@Entity
public class AirportInfo {
    private Integer id;
    private Integer airportId;
    private String name;
    private String city;
    private String state;
    private String country;

    public AirportInfo() {

    }

    public AirportInfo(Integer airportId, String name, String city, String state, String country) {
        this.airportId = airportId;
        this.name = name;
        this.city = city;
        this.state = state;
        this.country = country;
    }

    @Id
    @GeneratedValue(strategy=GenerationType.AUTO)
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}