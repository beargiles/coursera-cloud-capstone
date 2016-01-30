package com.coyotesong.coursera.cloud.repository;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityResult;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.persistence.SqlResultSetMapping;
import javax.persistence.SqlResultSetMappings;

import org.springframework.stereotype.Repository;

import com.coyotesong.coursera.cloud.domain.AirlineFlightDelays;

/**
 * Queries that can't be handled by standard Spring Data repository
 * inspection. Correction - they probably can be handled by the standard
 * code but I haven't found documentation on how to do it.
 * 
 * @author bgiles
 */
@Repository
@NamedQueries({
        @NamedQuery(name = "arrivalFlightDelaysByAirline", query = "select airline_id,count(1) as num_flights,sum(arrival_delay) as delay,sum(arrival_delay*arrival_delay) as delay_squared,max(arrival_delay) as max_delay from flight_info where cancelled=false and diverted=false group by airline_id order by airline_id") })
@SqlResultSetMappings({ @SqlResultSetMapping(name = "arrivalFlightDelaysByAirline", entities = {
        @EntityResult(entityClass = com.coyotesong.coursera.cloud.domain.AirlineFlightDelays.class) }) })
public class FlightInfoRepositoryImpl implements FlightInfoRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * @see com.coyotesong.coursera.cloud.repository.FlightInfoRepositoryCustom#listArrivalFlightDelaysByAirlineAsAirlineFlightDelays()
     */
    @Override
    public List<AirlineFlightDelays> listArrivalFlightDelaysByAirlineAsAirlineFlightDelays() {
        // Query query = entityManager.createNamedQuery("arrivalFlightDelaysByAirline", AirlineFlightDelays.class);
        Query query = entityManager.createNativeQuery("select airline_id,count(1) as num_flights,sum(arrival_delay) as delay,sum(arrival_delay*arrival_delay) as delay_squared,max(arrival_delay) as max_delay from flight_info where cancelled=false and diverted=false group by airline_id order by airline_id", AirlineFlightDelays.class);
        @SuppressWarnings("unchecked")
        List<AirlineFlightDelays> results = (List<AirlineFlightDelays>) query.getResultList();
        return results;
    }
}
