package com.jtangney.gcpex.dataflow.taxistream.bt;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import com.jtangney.gcpex.dataflow.taxistream.TaxiRide;

public class TaxiRideToBigtableFn extends DoFn<TaxiRide, KV<ByteString, Iterable<Mutation>>> {

  private static final long serialVersionUID = 1L;

  public static final String FAMILY_META = "meta";
  public static final String FAMILY_GEO = "geo";
  public static final String FAMILY_FARE = "fare";

  @ProcessElement
  public void processElement(ProcessContext c) {
    TaxiRide ride = c.element();
    ByteString rowKey = ByteString.copyFromUtf8(createRowKey(ride));
    Iterable<Mutation> list = createBtPut(ride);
    KV<ByteString, Iterable<Mutation>> kv = KV.of(rowKey, list);
    c.output(kv);
  }

  String createRowKey(TaxiRide ride) {
    ZonedDateTime zdt = ZonedDateTime.parse(ride.timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    long time = zdt.toEpochSecond();
    long reverseTime = Long.MAX_VALUE - time;
    return ride.rideId.concat("#").concat(String.valueOf(reverseTime));
  }

  private Iterable<Mutation> createBtPut(TaxiRide ride) {
    List<Mutation> mutes = new ArrayList<>();
    addSetCell(mutes, FAMILY_GEO, "lat", ride.lat);
    addSetCell(mutes, FAMILY_GEO, "lon", ride.lon);
    addSetCell(mutes, FAMILY_META, "ts", ride.timestamp);
    addSetCell(mutes, FAMILY_META, "status", ride.rideStatus);
    addSetCell(mutes, FAMILY_FARE, "meter", ride.meterReading);
    addSetCell(mutes, FAMILY_FARE, "meterinc", ride.meterIncrement);
    addSetCell(mutes, FAMILY_FARE, "psngrs", ride.passengerCount);

    return mutes;
  }

  private void addSetCell(List<Mutation> mutes, String colFamily, String colQualifier, Object value) {
    if (value == null) {
      return;
    }
    Mutation.SetCell setOp = Mutation.SetCell.newBuilder().setFamilyName(colFamily)
        .setColumnQualifier(ByteString.copyFromUtf8(colQualifier)).setValue(ByteString.copyFromUtf8(value.toString()))
        .build();
    Mutation mutation = Mutation.newBuilder().setSetCell(setOp).build();
    mutes.add(mutation);
  }
}
