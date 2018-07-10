package com.jtangney.gcpex.dataflow.taxistream.text;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

import com.jtangney.gcpex.dataflow.taxistream.BaseTaxiStreamPipelineOptions;

public interface TaxiStreamToTextPipelineOptions extends BaseTaxiStreamPipelineOptions {

  @Description("Output files written here")
  @Validation.Required
  String getOutputPath();

  void setOutputPath(String outputPath);

  @Description("Output file shards")
  @Default.Integer(1)
  int getOutputFileShards();

  void setOutputFileShards(int shards);

}
