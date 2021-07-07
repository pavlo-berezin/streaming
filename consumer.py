from kafka import KafkaConsumer
from json import loads
from PIL import Image
from io import BytesIO
from time import sleep, time
import datetime
import csv
import random


csvfile = open('logs/logs{}.csv'.format(random. randint(0,100)), 'w', newline='', encoding='utf-8')
c = csv.writer(csvfile)

c.writerow(['frame_n', 'sent', 'received', 'processed', 'size'])

consumer = KafkaConsumer(
    'videoparts',
    bootstrap_servers=['localhost:63427'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group')

for message in consumer:
  frame = [item for item in message.headers if item[0] == 'frame']

  if len(frame):
    frame = int.from_bytes(frame[0][1], 'big')
  else:
    frame = -1


  sent = message.timestamp
  received = int(time() * 1000)

  sleep(1)
  stream = BytesIO(message.value)
  image = Image.open(stream).convert("RGBA")
  size = stream.getbuffer().nbytes
  stream.close()

  processed = int(time() * 1000)
  print([frame, sent, received, processed, size])
  c.writerow([frame, sent, received, processed, size])