package com.jtangney.gcpex.dataflow.taxistream.bt;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

import com.jtangney.gcpex.dataflow.taxistream.BaseTaxiStreamPipelineOptions;

public interface TaxiStreamToBigtablePipelineOptions extends BaseTaxiStreamPipelineOptions {

  @Description("The Google Cloud Bigtable instance ID")
  @Default.String("btinst1")
  String getBigtableInstanceId();

  void setBigtableInstanceId(String bigtableInstanceId);

  @Description("The Cloud Bigtable table ID in the instance")
  @Default.String("taxirides")
  String getBigtableTableId();

  void setBigtableTableId(String bigtableTableId);
}
