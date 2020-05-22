# with open('input.csv','a') as f:
#     f.write('Counts\n')
#     for i in range(100):
#         f.write(str(i+1)+'\n')
import time

with open('input.csv','r') as f:
    line=f.readline()
    while line:
        line=f.readline()
        # print('Pulishing {0} to {1}'.format(line,pubsub_topic))
        # publisher.publish(pubsub_topic,line)
        print(line)
        time.sleep(1)