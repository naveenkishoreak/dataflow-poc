import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.bigquery import WriteToBigQuery

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

import json

#from transforms import ParsePubSubMessage

PROJECT_ID = 'naveen-dbt-project'
PUBSUB_TOPIC = 'projects/naveen-dbt-project/topics/naveen_dataflow'
BIGQUERY_TABLE = 'naveen-dbt-project:pubsub_dataflow.pubsub_sample_data'

pipeline_options = PipelineOptions([
    '--project=naveen-dbt-project',
    '--runner=DataflowRunner',
    '--temp_location=gs://naveen_dataflow_temp/temp',
    '--staging_location=gs://naveen_dataflow_staging/staging',
    '--region=us-central1',
    '--job_name=naveen-test-job',
    '--autoscaling_algorithm=THROUGHPUT_BASED',
    '--max_num_workers=1',
])

pipeline_options.view_as(StandardOptions).streaming = True


class ParsePubSubMessage(beam.DoFn):
    def process(self, element, *args, **kwargs):
        row = json.loads(element)
        yield row

# Defining the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    
    # Read from Pub/Sub
    pub_sub_data = (
        p
        | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=PUBSUB_TOPIC)
        | 'Decode Pub/Sub message' >> beam.Map(lambda x: x.decode('utf-8'))
        | 'Parse JSON message' >> beam.ParDo(ParsePubSubMessage())
        | 'Print to Console' >> beam.Map(lambda x: logging.info(x))
        
    )
    
    windowed_data = (
        pub_sub_data
        | 'Apply Windowing' >> beam.WindowInto(FixedWindows(60))
    )
    
    
    windowed_data | 'Write to BQ' >> WriteToBigQuery(
        BIGQUERY_TABLE,
        schema='id:INTEGER, value:STRING(20)', 
        write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED
    )