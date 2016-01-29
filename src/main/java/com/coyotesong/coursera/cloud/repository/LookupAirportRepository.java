package com.coyotesong.coursera.cloud.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.coyotesong.coursera.cloud.domain.LookupAirport;

@Repository
public interface LookupAirportRepository extends CrudRepository<LookupAirport, Integer> {

}
