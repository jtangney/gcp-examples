package com.jtangney.gcpex.dataflow.taxistream.bt;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Files;
import com.jtangney.gcpex.dataflow.taxistream.TaxiRide;
import com.jtangney.gcpex.dataflow.taxistream.bt.TaxiRideToBigtableFn;
import com.jtangney.gcpex.dataflow.taxistream.bt.TaxiStreamToBigtablePipeline;
import com.jtangney.gcpex.dataflow.taxistream.bt.TaxiStreamToBigtablePipelineOptions;
import com.jtangney.gcpex.dataflow.taxistream.text.TaxiStreamToTextPipeline;
import com.jtangney.gcpex.dataflow.taxistream.text.TaxiStreamToTextPipelineOptions;

public class TaxiStreamPipelineTest {

  @Ignore
  @Test
  public void testTextLocal() {
    TaxiStreamToTextPipelineOptions options = PipelineOptionsFactory.as(TaxiStreamToTextPipelineOptions.class);
    options.setRunner(DirectRunner.class);
    options.setProject("jt-demos");
    options.setStreaming(true);

    // write output to new temp dir
    File outdir = Files.createTempDir();
    System.out.println(outdir.getAbsolutePath());
    String outfilePrefix = options.getClass().getSimpleName();
    String outpath = String.format("%s/%s", outdir.getAbsolutePath(), outfilePrefix);
    options.setOutputPath(outpath);

    // run pipeline
    new TaxiStreamToTextPipeline(options).run();
  }

  @Test
  public void testCreateRowKey() {
    TaxiRideToBigtableFn fn = new TaxiRideToBigtableFn();
    TaxiRide ride = new TaxiRide();
    ride.timestamp = "2018-07-05T14:27:58.77008-04:00";
    ride.rideId = "someId";

    String key = fn.createRowKey(ride);
    System.out.println(key);
    assertTrue(key.startsWith("someId#"));
    assertTrue(key.matches("someId#\\d{10,}"));
  }

  @Ignore
  @Test
  public void testBigtableLocal() {
    TaxiStreamToBigtablePipelineOptions options = PipelineOptionsFactory.as(TaxiStreamToBigtablePipelineOptions.class);
    options.setRunner(DirectRunner.class);
    options.setProject("jt-demos");
    options.setStreaming(true);

    // run pipeline
    new TaxiStreamToBigtablePipeline(options).run();
  }

}
