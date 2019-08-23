
# coding: utf-8

# In[ ]:


import sys
import time
import cv2
import json
import decimal


import pytz
from pytz import timezone
import datetime


from kafka import KafkaProducer
from kafka.errors import KafkaError
import base64

topic = "image-pool"
brokers = ["kafka1-kafka-brokers:9092"]

camera_data = {'camera_id':'1',
               'position':'frontspace',
               'image_bytes':'123'}


# In[18]:


def convert_ts(ts, config):
    '''Converts a timestamp to the configured timezone. Returns a localized datetime object.'''
    #lambda_tz = timezone('US/Pacific')
    tz = timezone(config['timezone'])
    utc = pytz.utc

    utc_dt = utc.localize(datetime.datetime.utcfromtimestamp(ts))

    localized_dt = utc_dt.astimezone(tz)

    return localized_dt


def publish_camera():
    """
    Publish camera video stream to specified Kafka topic.
    Kafka Server is expected to be running on the localhost. Not partitioned.
    """

    # Start up producer


    producer = KafkaProducer(bootstrap_servers=brokers,
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    camera = cv2.VideoCapture(0)

    framecount = 0

    try:
        while(True):

            success, frame = camera.read()

            utc_dt = pytz.utc.localize(datetime.datetime.now())
            now_ts_utc = (utc_dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()

            ret, buffer = cv2.imencode('.jpg', frame)

            camera_data['image_bytes'] = base64.b64encode(buffer).decode('utf-8')

            camera_data['frame_count'] = str(framecount)

            camera_data['capture_time'] = str(now_ts_utc)

            producer.send(topic, camera_data)

            framecount = framecount + 1

            # Choppier stream, reduced load on processor
            time.sleep(0.002)


    except Exception as e:
        print((e))
        print("\nExiting.")
        sys.exit(1)


    camera.release()
    producer.close()



# In[19]:
def publish_video(video_file):
    """
    Publish given video file to a specified Kafka topic. 
    Kafka Server is expected to be running on the localhost. Not partitioned.
    
    :param video_file: path to video file <string>
    """
    # Start up producer
    producer = KafkaProducer(bootstrap_servers=brokers,
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Open file
    video = cv2.VideoCapture(video_file)
    #video.set(3, 640)
    #video.set(4, 480)
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter('output.mp4', fourcc, 60, (640, 480))

    print('publishing video...')
    
    framecount = 0 

    while(video.isOpened()):
        success, frame = video.read()
        b = cv2.resize(frame, (640, 480), fx=0, fy=0, interpolation = cv2.INTER_CUBIC)
        out.write(b)

        # Ensure file was read successfully
        if not success:
            print("bad read!")
            break
        
        # Convert image to png
        ret, buffer = cv2.imencode('.jpg', b)
        
        utc_dt = pytz.utc.localize(datetime.datetime.now())
        now_ts_utc = (utc_dt - datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()

        camera_data['image_bytes'] = base64.b64encode(buffer).decode('utf-8')

        camera_data['frame_count'] = str(framecount)

        camera_data['capture_time'] = str(now_ts_utc)
        
        
        

        # Convert to bytes and send to kafka
        producer.send(topic, camera_data)

        framecount = framecount + 1
        time.sleep(0.02)
    video.release()
    out.release()
    cv2.destroyAllWindows()
    print('publish complete')


if __name__ == "__main__":
    publish_video("1.mp4")
