import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions
import os
from apache_beam import window



path_service_account = 'beamdemo-277311-12b8f9ad6495.json'

#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_service_account


input_subscription = 'projects/beamdemo-277311/subscriptions/Subscribe1'

output_topic = 'projects/beamdemo-277311/topics/Topic2'

options = PipelineOptions()

options.view_as(StandardOptions).streaming = True

p=beam.Pipeline(options=options)

output_file = './output/new_data'

pubsub_data = (
    p
    |"Read from pubsub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
    #|"Write to pubsub" >> beam.io.WriteToPubSub(topic=output_topic)
    |"Write to file" >> beam.io.WriteToText(output_file)

)
result = p.run()
result.wait_until_finish()