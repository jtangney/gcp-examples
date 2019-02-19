package com.jtangney.gcpex.dataflow.taxistream.bt;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface BigtableOptions extends DataflowPipelineOptions {

  @Description("The Google Cloud Bigtable instance ID")
  @Default.String("btinst1")
  String getBigtableInstanceId();

  void setBigtableInstanceId(String bigtableInstanceId);

  @Description("The Cloud Bigtable table ID in the instance")
  @Default.String("taxirides")
  String getBigtableTableId();

  void setBigtableTableId(String bigtableTableId);

}
