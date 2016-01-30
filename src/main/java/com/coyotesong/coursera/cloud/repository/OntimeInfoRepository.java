package com.coyotesong.coursera.cloud.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.coyotesong.coursera.cloud.domain.OntimeInfo;

@Repository
public interface OntimeInfoRepository extends JpaRepository<OntimeInfo, Long> {
    
   long countByDestAirportId(Integer id);

   long countByOriginAirportId(Integer id);
   
   @Query(value="select dest_airport_id from ontime_info group by dest_airport_id order by count(1) desc", nativeQuery=true)
   List<Integer> listPopularDestAirportIds();
   
   @Query(value="select origin_airport_id from ontime_info group by origin_airport_id order by count desc", nativeQuery=true)
   List<Integer> listPopularOriginAirportIds();
}
