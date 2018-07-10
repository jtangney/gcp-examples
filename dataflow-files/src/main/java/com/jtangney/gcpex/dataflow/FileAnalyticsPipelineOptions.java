package com.jtangney.gcpex.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface FileAnalyticsPipelineOptions extends DataflowPipelineOptions {
  @Description("Location of input files")
  String getFileSource();

  void setFileSource(String source);

  @Description("Location of output files")
  String getFileSink();

  void setFileSink(String destination);

}
