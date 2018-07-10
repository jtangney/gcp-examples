package com.jtangney.gcpex.dataflow.taxistream;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

public class TaxiRide implements Serializable {

  private static final long serialVersionUID = 3343701768059191560L;

  // ride format from PubSub
  // {
  // "ride_id":"a60ba4d8-1501-4b5b-93ee-b7864304d0e0",
  // "latitude":40.66684000000033,
  // "longitude":-73.83933000000202,
  // "timestamp":"2016-08-31T11:04:02.025396463-04:00",
  // "meter_reading":14.270274,
  // "meter_increment":0.019336415,
  // "ride_status":"enroute" / "pickup" / "dropoff"
  // "passenger_count":2
  // }

  @SerializedName("ride_id")
  public String rideId;
  @SerializedName("point_idx")
  public Integer pointIdx;
  @SerializedName("latitude")
  public Double lat;
  @SerializedName("longitude")
  public Double lon;
  @SerializedName("timestamp")
  public String timestamp;
  @SerializedName("meter_reading")
  public Double meterReading;
  @SerializedName("meter_increment")
  public Double meterIncrement;
  @SerializedName("ride_status")
  public String rideStatus;
  @SerializedName("passenger_count")
  public Integer passengerCount;

  public TaxiRide() {
  }
}
