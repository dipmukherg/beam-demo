import os
import time
from google.cloud import pubsub_v1

if __name__=="__main__":
    project = 'beamdemo-277311'
    pubsub_topic = 'projects/beamdemo-277311/topics/Topic1'

    path_service_account = 'beamdemo-277311-12b8f9ad6495.json'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_service_account

    input_file = 'input.csv'

    publisher = pubsub_v1.PublisherClient()

    with open(input_file,'rb') as f:
        line=f.readline()
        for line in f:
            print('Pulishing {0} to {1}'.format(line,pubsub_topic))
            publisher.publish(pubsub_topic,line)
            time.sleep(1)
