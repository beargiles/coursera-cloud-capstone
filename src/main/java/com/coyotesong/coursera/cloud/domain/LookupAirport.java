package com.coyotesong.coursera.cloud.domain;

import javax.persistence.Entity;

/**
 * Static lookup information about airports, from L_AIRPORT_ID.csv
 * 
 * @author bgiles
 */
@Entity
public class LookupAirport {
    private Integer code;
    private String name;
    private String location;

    public LookupAirport() {

    }

    public LookupAirport(Integer code, String name, String location) {
        this.code = code;
        this.name = name;
        this.location = location;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((code == null) ? 0 : code.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LookupAirport other = (LookupAirport) obj;
        if (code == null) {
            if (other.code != null)
                return false;
        } else if (!code.equals(other.code))
            return false;
        if (location == null) {
            if (other.location != null)
                return false;
        } else if (!location.equals(other.location))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "LookupAirport [code=" + code + ", name=" + name + ", location=" + location + "]";
    }
}
