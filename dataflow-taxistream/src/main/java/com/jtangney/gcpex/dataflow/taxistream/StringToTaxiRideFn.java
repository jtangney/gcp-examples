package com.jtangney.gcpex.dataflow.taxistream;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.gson.Gson;

public class StringToTaxiRideFn extends DoFn<String, TaxiRide> {

  private static final long serialVersionUID = 1L;

  public StringToTaxiRideFn() {
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String element = c.element();
    Gson gson = new Gson();
    TaxiRide ride = gson.fromJson(element, TaxiRide.class);
    c.output(ride);
  }

}
