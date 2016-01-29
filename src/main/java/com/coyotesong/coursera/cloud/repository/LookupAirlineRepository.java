package com.coyotesong.coursera.cloud.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.coyotesong.coursera.cloud.domain.LookupAirline;

@Repository
public interface LookupAirlineRepository extends CrudRepository<LookupAirline, Integer> {

}
