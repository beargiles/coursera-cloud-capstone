package com.coyotesong.coursera.cloud.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.coyotesong.coursera.cloud.domain.FlightInfo;

/**
 * Ontime performance information.
 * 
 * Query that returns multiple values: - collect statistics corresponding to
 * AirlineFlightDelaysWritable - select
 * airline_id,count(1),sum(arrival_delay),sum(arrival_delay*arrival_delay),max(
 * arrival_delay) from ontime_info where cancelled=false and diverted=false
 * group by airline_id order by airline_id;
 * 
 * @author bgiles
 */
@Repository
public interface FlightInfoRepository extends JpaRepository<FlightInfo, Long>, FlightInfoRepositoryCustom {

    long countByDestAirportId(Integer id);

    long countByOriginAirportId(Integer id);

    /**
     * List popular destination airports.
     * 
     * @return
     */
    @Query(value = "select dest_airport_id from flight_info where cancelled = false and diverted = false group by dest_airport_id order by count(1) desc", nativeQuery = true)
    List<Integer> listPopularDestAirportIds();

    /**
     * List popular origin airports.
     * @return
     */
    @Query(value = "select origin_airport_id from flight_info where cancelled = false and diverted = false group by origin_airport_id order by count desc", nativeQuery = true)
    List<Integer> listPopularOriginAirportIds();

    /**
     * List all flights by (origin id, destination id, airline id) tuples.
     * 
     * @param originId
     * @param destinationId
     * @param airportId
     * @return
     */
    List<FlightInfo> findByOriginAirportIdAndDestAirportIdAndAirlineId(Integer originId, Integer destinationId,
            Integer airportId);
}
