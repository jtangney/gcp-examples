package com.jtangney.gcpex.dataflow.taxistream.bt;

import java.util.logging.Logger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import com.jtangney.gcpex.dataflow.taxistream.StringToTaxiRideFn;
import com.jtangney.gcpex.dataflow.taxistream.text.TaxiStreamToTextPipeline;

public class TaxiStreamToBigtablePipeline {
  private static final Logger LOG = Logger.getLogger(TaxiStreamToTextPipeline.class.getName());

  private final TaxiStreamToBigtablePipelineOptions options;

  public TaxiStreamToBigtablePipeline(TaxiStreamToBigtablePipelineOptions options) {
    this.options = options;
  }

  public PipelineResult run() {
    Pipeline p = Pipeline.create(options);

    FixedWindows window = FixedWindows.of(Duration.standardSeconds(options.getWindowSeconds()));

    BigtableIO.Write btWrite = BigtableIO.write().withProjectId(options.getProject())
        .withInstanceId(options.getBigtableInstanceId())
        .withTableId(options.getBigtableTableId());

    // read messages as Strings from topic
    p.apply(PubsubIO.readStrings().fromSubscription(options.getSubscription())
        .withTimestampAttribute(options.getTimestampAttribute()))
        // apply some fixed Window
        .apply(Window.<String> into(window))
        // Transform Strings to objects
        .apply(ParDo.of(new StringToTaxiRideFn()))
        // create Bigtable operations
        .apply(ParDo.of(new TaxiRideToBigtableFn()))
        // insert into Bigtable
        .apply(btWrite);

    // deploy and execute
    return p.run();
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(TaxiStreamToBigtablePipelineOptions.class);
    TaxiStreamToBigtablePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(TaxiStreamToBigtablePipelineOptions.class);

    new TaxiStreamToBigtablePipeline(options).run();
  }
}

