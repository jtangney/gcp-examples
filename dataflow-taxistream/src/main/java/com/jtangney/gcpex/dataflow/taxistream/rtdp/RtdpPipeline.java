package com.jtangney.gcpex.dataflow.taxistream.rtdp;

import java.io.Serializable;
import java.util.logging.Logger;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import com.jtangney.gcpex.dataflow.taxistream.bt.BigtableOptions;
import com.jtangney.gcpex.dataflow.taxistream.bt.TaxiStreamToBigtablePipelineOptions;

public class RtdpPipeline {

  private static final Logger LOG = Logger.getLogger(RtdpPipeline.class.getName());

  private final RtdpPipelineOptions options;

  public interface RtdpPipelineOptions extends BigtableOptions, DataflowPipelineOptions {

    @Description("IoT Core telemetry topic")
    @Validation.Required
    String getTopic();

    void setTopic(String topic);
  }

  public RtdpPipeline(RtdpPipelineOptions options) {
    this.options = options;
  }

  public PipelineResult run() {
    Pipeline p = Pipeline.create(options);

    FixedWindows window = FixedWindows.of(Duration.standardSeconds(30));
    String fullTopicName = String.format("projects/%s/topics/%s", options.getProject(), options.getTopic());
    BigtableIO.Write btWrite = BigtableIO.write().withProjectId(options.getProject())
        .withInstanceId(options.getBigtableInstanceId()).withTableId(options.getBigtableTableId());

    // read messages as Strings from topic
    PCollection<String> messages = p.apply(PubsubIO.readStrings().fromTopic(fullTopicName));

    // Branch the pipeline!
    // First branch, just write through to Bigtable
    messages.apply(ParDo.of(new TelemetryToBigtableFn())).apply(btWrite);

    // Second branch, group to get average sensor temperature over some period
//    messages.apply(Window.<String> into(window))
    // extract the device Ids
    PCollection<KV<String, String>> byId = messages.apply(ParDo.of(new ExtractDeviceIdFn()));
    // group by device Id
    PCollection<KV<String, Iterable<String>>> grouped = byId.apply(GroupByKey.create());
    // calculate some stats over the group
    PCollection<KV<String, DeviceSummaryStats>> keyedStats = grouped.apply(ParDo.of(new AggregateStats()));

    // deploy and execute
    return p.run();
  }

  public PipelineResult runSimple(String inputFiles, String outputPath) {
    Pipeline p = Pipeline.create(options);

    PCollection<String> messages = p.apply(TextIO.read().from(inputFiles));
    PCollection<KV<String, String>> byId = messages.apply(ParDo.of(new ExtractDeviceIdFn()));
    // group by device Id
    PCollection<KV<String, Iterable<String>>> grouped = byId.apply(GroupByKey.create());
    // calculate some stats over the group
    PCollection<KV<String, DeviceSummaryStats>> keyedStats = grouped.apply(ParDo.of(new AggregateStats()));
    
    throw new RuntimeException("Unfinished implmentation");
//    keyedStats.apply(TextIO.write().to(outputPath));
//
//    // deploy and execute
//    return p.run();
  }

  static class ExtractDeviceIdFn extends DoFn<String, KV<String, String>> {

    private static final long serialVersionUID = 4526391188281569031L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      String element = c.element();
      String[] items = element.split(",");
      String id = items[0];
      KV<String, String> outputValue = KV.of(id, element);
      c.output(outputValue);
    }
  }

  static class AggregateStats extends DoFn<KV<String, Iterable<String>>, KV<String, DeviceSummaryStats>> {

    private static final long serialVersionUID = -7086250666437250366L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, Iterable<String>> element = c.element();
      String key = element.getKey();
      double totalTemp = 0;
      int count = 0;
      for (String s : element.getValue()) {
        String[] elements = s.split(",");
        double temp = Double.parseDouble(elements[2]);
        count++;
        totalTemp += temp;
      }
      double averageTemp = totalTemp / count;
      DeviceSummaryStats stats = new DeviceSummaryStats(averageTemp);
      KV<String, DeviceSummaryStats> outputValue = KV.of(key, stats);
      c.output(outputValue);
    }
  }
  
  static class DeviceSummaryStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private double avgTemp;

    public DeviceSummaryStats(double d) {
      this.avgTemp = d;
    }

    public double getAvgTemp() {
      return this.avgTemp;
    }
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(TaxiStreamToBigtablePipelineOptions.class);
    RtdpPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(RtdpPipelineOptions.class);

    new RtdpPipeline(options).run();
  }

}
