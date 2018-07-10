package com.jtangney.gcpex.dataflow.taxistream;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface BaseTaxiStreamPipelineOptions extends DataflowPipelineOptions {

  @Description("Topic with NYC Taxi data")
  @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
  @Validation.Required
  String getTopic();

  void setTopic(String topic);

  @Description("Subscription with NYC Taxi data")
  @Default.String("projects/jt-demos/subscriptions/taxisub")
  @Validation.Required
  String getSubscription();

  void setSubscription(String subscription);

  @Description("Pubsub message attribute that contains message timestamp used for windowing etc")
  @Default.String("ts")
  @Validation.Required
  String getTimestampAttribute();

  void setTimestampAttribute(String attr);

  @Description("Beam Window seconds")
  @Default.Integer(5)
  int getWindowSeconds();

  void setWindowSeconds(int secs);

}
