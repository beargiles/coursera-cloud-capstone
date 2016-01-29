package com.coyotesong.coursera.cloud.domain;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Static lookup information about airlines, from L_AIRLINE_ID.csv
 * 
 * @author bgiles
 */
@Entity
public class LookupAirline {
    private Integer code;
    private String name;
    private String abbreviation;
    
    public LookupAirline() {
        
    }
    
    public LookupAirline(Integer code, String name, String abbreviation) {
        this.code = code;
        this.name = name;
        this.abbreviation = abbreviation;
    }

    @Id
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

    public String getAbbreviation() {
        return abbreviation;
    }

    public void setAbbreviation(String abbreviation) {
        this.abbreviation = abbreviation;
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
        LookupAirline other = (LookupAirline) obj;
        if (code == null) {
            if (other.code != null)
                return false;
        } else if (!code.equals(other.code))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (abbreviation == null) {
            if (other.abbreviation != null)
                return false;
        } else if (!abbreviation.equals(other.abbreviation))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Airline [code=" + code + ", name=" + name + ", abbreviation=" + abbreviation + "]";
    }
}
