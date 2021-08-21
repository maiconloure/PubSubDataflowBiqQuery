import argparse
import json
import os
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

# Service account key path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/[PROJECT_ID].json"
INPUT_SUBSCRIPTION = "projects/[PROJECT_ID]/subscriptions/[SUBSCRIPTION_ID]"
BIGQUERY_TABLE = "[PROJECT_ID]:teste.example"
BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"


class CustomParsing(beam.DoFn):
    """ Custom ParallelDo class to apply a custom transformation """

    def to_runner_api_parameter(self, unused_context):
        # Not very relevant, returns a URN (uniform resource name) and the payload
        return "beam:transforms:custom_parsing:custom_v0", None

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        """
        Simple processing function to parse the data and add a timestamp
        For additional params see:
        https://beam.apache.org/releases/pydoc/2.7.0/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn
        """
        parsed = json.loads(element.decode("utf-8"))
        parsed["timestamp"] = timestamp.to_rfc3339()
        yield parsed


def run():
    # Parsing arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help='Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
        default=INPUT_SUBSCRIPTION,
    )
    parser.add_argument(
        "--output_table", help="Output BigQuery Table", default=BIGQUERY_TABLE
    )
    parser.add_argument(
        "--output_schema",
        help="Output BigQuery Schema in text format",
        default=BIGQUERY_SCHEMA,
    )
    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = '[PROJECT_ID]'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.staging_location = 'gs://[BUCKET_NAME]/staging'
    google_cloud_options.temp_location = 'gs://[BUCKET_NAME]/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(StandardOptions).streaming = True


    # Defining our pipeline and its steps
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=known_args.input_subscription, timestamp_attribute=None
            )
            | "CustomParse" >> beam.ParDo(CustomParsing())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()