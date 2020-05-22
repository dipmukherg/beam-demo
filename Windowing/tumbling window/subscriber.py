import os
import time
from google.cloud import pubsub_v1


if __name__=="__main__":

    path_service_account = 'beamdemo-277311-12b8f9ad6495.json'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_service_account

    subscription_path = 'projects/beamdemo-277311/subscriptions/Subscribe2'


    subscriber = pubsub_v1.SubscriberClient()

    def callback(message):
        print('Received Message {}'.format(message))
        message.ack()

    subscriber.subscribe(subscription_path,callback=callback)
    while True:
        time.sleep(60)