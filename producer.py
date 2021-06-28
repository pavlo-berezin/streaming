from time import sleep
from json import dumps
from kafka import KafkaProducer
import cv2


producer = KafkaProducer(bootstrap_servers=['localhost:63427'])

vidcap = cv2.VideoCapture('video.mp4')
success,image = vidcap.read()
count = 1
while success:
  ret, buffer = cv2.imencode('.jpg', image)
  producer.send("videoparts", buffer.tobytes(), headers=[('frame', (count).to_bytes(2, byteorder='big'))])

  success,image = vidcap.read()
  print('Read a new frame: ', success)
  count += 1