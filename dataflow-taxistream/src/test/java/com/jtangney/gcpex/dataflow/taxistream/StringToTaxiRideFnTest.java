package com.jtangney.gcpex.dataflow.taxistream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.google.gson.Gson;
import com.jtangney.gcpex.dataflow.taxistream.TaxiRide;

public class StringToTaxiRideFnTest {

  public String RIDE_JSON1 = "{'ride_id':'652b1bf0-f378-4ef0-9dc0-49136ae85c9b','point_idx':958,'latitude':40.71208,'longitude':-73.82275, "
      + "'timestamp':'2018-07-05T12:57:30.00174-04:00','meter_reading':21.159754,'meter_increment':0.022087425,"
      + "'ride_status':'enroute','passenger_count':1}";

  // remove some fields to test nulls
  public String RIDE_JSON2 = "{'ride_id':'652b1bf0-f378-4ef0-9dc0-49136ae85c9b','latitude':40.71208,'longitude':-73.82275, "
      + "'timestamp':'2018-07-05T12:57:30.00174-04:00','meter_reading':21.159754,'meter_increment':0.022087425,"
      + "'passenger_count':1}";

  @Test
  public void testParse() {
    Gson gson = new Gson();
    TaxiRide ride = gson.fromJson(RIDE_JSON1, TaxiRide.class);
    assertNotNull(ride);
    assertEquals("652b1bf0-f378-4ef0-9dc0-49136ae85c9b", ride.rideId);
    assertEquals(958, ride.pointIdx.intValue());
    assertEquals(-73.82275, ride.lon.doubleValue(), 0.0001);
    assertEquals("2018-07-05T12:57:30.00174-04:00", ride.timestamp);

    ride = gson.fromJson(RIDE_JSON2, TaxiRide.class);
    assertNotNull(ride);
    assertEquals("652b1bf0-f378-4ef0-9dc0-49136ae85c9b", ride.rideId);
    assertNull(ride.pointIdx);
    assertNull(ride.rideStatus);
  }

}
