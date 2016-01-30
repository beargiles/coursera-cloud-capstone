package com.coyotesong.coursera.cloud.repository;

import java.util.List;

import com.coyotesong.coursera.cloud.domain.AirlineFlightDelays;

public interface FlightInfoRepositoryCustom {

    /**
     * List arrival flight delays by airline.
     */
    List<AirlineFlightDelays> listArrivalFlightDelaysByAirlineAsAirlineFlightDelays();

}