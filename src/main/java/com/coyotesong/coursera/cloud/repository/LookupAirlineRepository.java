package com.coyotesong.coursera.cloud.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.coyotesong.coursera.cloud.domain.LookupAirline;
import com.coyotesong.coursera.cloud.domain.LookupAirport;

@Repository
public interface LookupAirlineRepository extends CrudRepository<LookupAirline, Integer> {
    Optional<LookupAirport> findByAbbreviation(String location);
    
    List<LookupAirport> findByName(String name);
}
