package com.jtangney.gcpex.dataflow.taxistream.text;

import java.util.logging.Logger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class TaxiStreamToTextPipeline {

  private static final Logger LOG = Logger.getLogger(TaxiStreamToTextPipeline.class.getName());

  private final TaxiStreamToTextPipelineOptions options;

  public TaxiStreamToTextPipeline(TaxiStreamToTextPipelineOptions options) {
    this.options = options;
  }

  public PipelineResult run() {
    Pipeline p = Pipeline.create(options);
    
    // read messages as Strings from topic
    PCollection<String> messages = p
        .apply(PubsubIO.readStrings().fromSubscription(options.getSubscription())
            .withTimestampAttribute(options.getTimestampAttribute()));
    // apply some fixed Window
    PCollection<String> windowedMsgs = messages
        .apply(Window.<String> into(FixedWindows.of(Duration.standardSeconds(options.getWindowSeconds()))));
    // write them out, also on a fixed Window
    windowedMsgs.apply(
        TextIO.write().withWindowedWrites().withNumShards(options.getOutputFileShards()).to(options.getOutputPath()));

    // deploy and execute
    return p.run();
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(TaxiStreamToTextPipelineOptions.class);
    TaxiStreamToTextPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(TaxiStreamToTextPipelineOptions.class);

    new TaxiStreamToTextPipeline(options).run();
  }
}
