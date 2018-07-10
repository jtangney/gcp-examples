package com.jtangney.gcpex.dataflow;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.logging.v2.model.LogEntry;

/**
 * Simple Dataflow Pipeline demonstrating how to process individual files as
 * single PCollection elements.
 * <p>
 * Use case is when pipeline inputs are entire files; the information for a
 * single 'record' is split over many lines within a single file, rather than
 * each individual line within the file representing a record (as with TextIO).
 * 
 * @author jtangney
 */
public class FileAnalyticsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(FileAnalyticsPipeline.class);

  private static class ParseLogMessageFn extends DoFn<ReadableFile, SimpleLogMessage> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      ReadableFile meta = c.element();
      String filename = meta.getMetadata().resourceId().getFilename();
      LOG.info(filename);
      String content;
      try {
        content = meta.readFullyAsUTF8String();
        c.output(parseFile(content));
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }

    private SimpleLogMessage parseFile(String entry) {
      try {
        JsonParser parser = new JacksonFactory().createJsonParser(entry);
        LogEntry logEntry = parser.parse(LogEntry.class);
        Integer status = logEntry.getHttpRequest().getStatus();
        String insert = logEntry.getInsertId();
        return new SimpleLogMessage(status, insert);
      } catch (IOException e) {
        LOG.error("IOException parsing entry: " + e.getMessage());
      } catch (NullPointerException e) {
        LOG.error("NullPointerException parsing entry: " + e.getMessage());
      }
      return null;
    }
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(FileAnalyticsPipelineOptions.class);
    FileAnalyticsPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(FileAnalyticsPipelineOptions.class);

    run(options);
  }

  // testing
  static void run(FileAnalyticsPipelineOptions options) {
    LOG.info("Starting pipeline...");
    LOG.info("Reading input files from " + options.getFileSource());
    LOG.info("Output files will be written to " + options.getFileSink());
    Pipeline p = Pipeline.create(options);

    // step by step for clarity

    // match files in the input dir
    PCollection<Metadata> matches = p.apply("Match", FileIO.match().filepattern(options.getFileSource()));
    // read each file in its entirety
    PCollection<ReadableFile> readables = matches.apply(FileIO.readMatches());
    // extract content and parse into domain obj
    PCollection<SimpleLogMessage> logs = readables.apply(ParDo.of(new ParseLogMessageFn()));
    // call toString on the objs
    PCollection<String> logStrings = logs.apply(ToString.elements());
    // write out files to the output dir
    logStrings.apply(TextIO.write().to(options.getFileSink()));

    PipelineResult r = p.run();
    LOG.info(r.toString());
  }
}
