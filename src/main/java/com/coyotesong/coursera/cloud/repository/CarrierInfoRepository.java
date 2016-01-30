package com.coyotesong.coursera.cloud.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.coyotesong.coursera.cloud.domain.CarrierInfo;

@Repository
public interface CarrierInfoRepository extends JpaRepository<CarrierInfo, Integer> {

}
