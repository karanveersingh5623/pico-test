
# coding: utf-8

# In[ ]:
# Boto3 is the Amazon Web Services (AWS) Software Development Kit (SDK) for Python, which allows Python developers to write software that makes use of services like Amazon S3 and Amazon EC2. 
# OpenCV is a library of programming functions mainly aimed at real-time computer vision


from __future__ import print_function
import boto3
import json
import cv2
import decimal
from copy import deepcopy

#from __future__ import print_function
import base64
import datetime
import time
import decimal
import uuid
import json
import boto3
import pytz
from pytz import timezone
from copy import deepcopy

from PIL import Image, ImageDraw, ExifTags, ImageColor, ImageFont

import datetime
from kafka import KafkaConsumer, KafkaProducer
import boto3
import json
import base64
import io

# Fire up the Kafka Consumer
topic = "image-pool"
brokers = ["kafka1-kafka-brokers:9092"]

consumer = KafkaConsumer(
    topic, 
    bootstrap_servers=brokers,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')))


# In[18]:

producer = KafkaProducer(bootstrap_servers=brokers,
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))



def load_config():
    '''Load configuration from file.'''
    with open('image-processor.json', 'r') as conf_file:
        conf_json = conf_file.read()
        return json.loads(conf_json)

#Load config
config = load_config()

def start_processor():
    
    while True:
        
#         raw_frame_messages = consumer.poll(timeout_ms=10, max_records=10)
        raw_frame_messages = consumer.poll()
        
        for topic_partition, msgs in raw_frame_messages.items():
            for msg in msgs:

                camera_data = {}

                img_bytes = base64.b64decode(msg.value['image_bytes'])

                camera_topic = "camera"+str(msg.value['camera_id'])

                stream = io.BytesIO(img_bytes)
                image=Image.open(stream)

                imgWidth, imgHeight = image.size  
                draw = ImageDraw.Draw(image) 
                
                imgByteArr = io.BytesIO()
                image.save(imgByteArr, format=image.format)
                imgByteArr = imgByteArr.getvalue()


                camera_data['image_bytes'] = base64.b64encode(imgByteArr).decode('utf-8')

        #             print(camera_topic)

                producer.send(camera_topic,camera_data)
        

if __name__ == "__main__":
    start_processor()


# In[ ]:




