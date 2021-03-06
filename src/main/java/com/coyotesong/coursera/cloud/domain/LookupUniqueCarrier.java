package com.coyotesong.coursera.cloud.domain;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Static lookup information on unique carriers, from L_UNIQUE_CARRIERS_ID.csv
 * 
 * @author bgiles
 */
@Entity
public class LookupUniqueCarrier {
    private String code;
    private String description;

    @Id
    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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
        LookupUniqueCarrier other = (LookupUniqueCarrier) obj;
        if (code == null) {
            if (other.code != null)
                return false;
        } else if (!code.equals(other.code))
            return false;
        if (description == null) {
            if (other.description != null)
                return false;
        } else if (!description.equals(other.description))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "LookupUniqueCarrier [code=" + code + ", description=" + description + "]";
    }
}
