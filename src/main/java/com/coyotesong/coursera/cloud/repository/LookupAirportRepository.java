package com.coyotesong.coursera.cloud.repository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.coyotesong.coursera.cloud.domain.LookupAirport;

@Repository
public interface LookupAirportRepository extends CrudRepository<LookupAirport, Integer> {
    List<LookupAirport> findByLocation(String location);
    
    List<LookupAirport> findByName(String name);
}
