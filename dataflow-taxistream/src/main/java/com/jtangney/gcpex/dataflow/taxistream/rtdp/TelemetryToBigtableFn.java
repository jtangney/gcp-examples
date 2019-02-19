package com.jtangney.gcpex.dataflow.taxistream.rtdp;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;

public class TelemetryToBigtableFn extends DoFn<String, KV<ByteString, Iterable<Mutation>>> {

  private static final long serialVersionUID = 5313298354912054952L;

  public static final String FAMILY_CF1 = "cf1";

  @ProcessElement
  public void processElement(ProcessContext c) {
    String telemtry = c.element();
    String[] msg = telemtry.split(",");

    ByteString rowKey = ByteString.copyFromUtf8(createRowKey(msg));
    Iterable<Mutation> list = createBtPut(msg);
    KV<ByteString, Iterable<Mutation>> kv = KV.of(rowKey, list);
    c.output(kv);
  }

  String createRowKey(String[] msg) {
    long time = Long.parseLong(msg[1]);
    long reverseTime = Long.MAX_VALUE - time;
    return msg[0].concat("#").concat(String.valueOf(reverseTime));
  }

  String epochMillisToIso(long millis) {
    ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"));
    return zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
  }

  private Iterable<Mutation> createBtPut(String[] msg) {
    List<Mutation> mutes = new ArrayList<>();
    long time = Long.parseLong(msg[1]);
    String ts = this.epochMillisToIso(time);
    addSetCell(mutes, FAMILY_CF1, "ts", ts);
    addSetCell(mutes, FAMILY_CF1, "temp", msg[2]);
    addSetCell(mutes, FAMILY_CF1, "lat", msg[3]);
    addSetCell(mutes, FAMILY_CF1, "lon", msg[4]);
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
