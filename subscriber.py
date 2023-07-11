import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os, logging, json

# Set the Pub/Sub topic and output path
project_id = "ps-int-datateamrnd-22072022"
subscription_id = "pubsub-test-lingga-sub"
# output_path = "gs://bucket_lingga/output/"
credentials = "D:\WORK\lingga-sa-int-datateamrnd-22072022-1e041e96c9a0.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials

# BQ table
table_id = 'stream_pubsub'  # replace with your table ID 
dataset_id = 'lingga_test'  # replace with your dataset_id

#BQ Schema
bq_schema = schema_stdetails_fact = (
    'message:STRING'
    )

#dataflow options
job_name = "stream-test-2" # replace with your job name
temp_location=f'gs://bucket_lingga/dataflow_temp'
staging_location = f'gs://bucket_lingga/dataflow_staging' # replace with  your folder destination
max_num_workers=1 # replace with preferred num_workers
worker_region='asia-southeast2' #replace with your worker region
streaming=True

def run(argv=None):

    options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        job_name=job_name,
        temp_location=temp_location,
        region=worker_region,
        autoscaling_algorithm='THROUGHPUT_BASED',
        max_num_workers=max_num_workers,
        save_main_session = True,
        streaming=True
    )
    
    # p = beam.Pipeline(options=PipelineOptions())
    p = beam.Pipeline(options=options) #pake pipelines options

    def extract_message_content(message):
        json_data = json.loads(message.data.decode('utf-8'))
        return json_data['message']

    pubsub_message = (p 
                        | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_id}', with_attributes=True)
                        | 'Extract message content' >> beam.Map(extract_message_content)
                        | 'Process messages' >> beam.Map(lambda message: {"message": message})
                        | 'Write to BQ' >> beam.io.WriteToBigQuery(
                                table=table_id,
                                dataset=dataset_id,
                                project=project_id,
                                schema=bq_schema,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                            )
                    )
    
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
#     # Set the logger
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=logging.INFO)
    # Run the core pipeline
    logging.info('Starting')
    run()
