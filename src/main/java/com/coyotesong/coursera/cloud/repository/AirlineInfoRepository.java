package com.coyotesong.coursera.cloud.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.coyotesong.coursera.cloud.domain.AirlineInfo;

@Repository
public interface AirlineInfoRepository extends JpaRepository<AirlineInfo, Integer> {

}
