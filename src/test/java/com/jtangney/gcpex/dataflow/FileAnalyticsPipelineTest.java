package com.jtangney.gcpex.dataflow;

import java.io.File;
import java.net.URL;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Files;

public class FileAnalyticsPipelineTest {

  private static final String PROJECT = "";
  private static final String BUCKET = "";

  @Test
  public void runLocal() {
    FileAnalyticsPipelineOptions options = PipelineOptionsFactory.as(FileAnalyticsPipelineOptions.class);
    File tmpdir = Files.createTempDir();
    URL currentdir = this.getClass().getResource("");

    options.setRunner(DirectRunner.class);
    options.setFileSource(String.format("%s%s", currentdir.getPath(), "*.json"));
    options.setFileSink(String.format("%s/%s", tmpdir.getAbsolutePath(), this.getClass().getSimpleName()));

    FileAnalyticsPipeline.run(options);
  }

  @Ignore // integration test
  @Test
  public void runCloud() {
    FileAnalyticsPipelineOptions options = PipelineOptionsFactory.as(FileAnalyticsPipelineOptions.class);

    // For Cloud execution, set the Cloud Platform project, staging location,
    // and specify DataflowRunner.
    options.setRunner(DataflowRunner.class);
    options.setProject(PROJECT);
    options.setStagingLocation(String.format("%s/staging", BUCKET));
    options.setTempLocation(String.format("%s/temp", BUCKET));
    options.setFileSource(String.format("%s/files/input/*.json", BUCKET));
    options.setFileSink(String.format("%s/files/output/", BUCKET));

    FileAnalyticsPipeline.run(options);
  }
}
