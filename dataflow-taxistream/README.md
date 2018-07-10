# Dataflow Taxi Stream
Example streaming dataflow pipelines that consume simulated NYC taxi rides.
See GCP blog [post](https://cloud.google.com/blog/big-data/2017/01/learn-real-time-processing-with-a-new-public-data-stream-and-google-cloud-dataflow-codelab) for details on the data stream

Two flavours
 * output sink simple text files (e.g. in GCS)
 * output sink is Bigtable
 
 Config defined in the pom - update with your own GCP resources.
 You'll need to create a subscription in your own Project that consumes from the public topic in blog post above. 