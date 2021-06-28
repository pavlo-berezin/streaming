from kafka import KafkaConsumer
from json import loads
from PIL import Image
from io import BytesIO
import datetime
from time import sleep, time
import csv


csvfile = open('logs.csv', 'w', newline='', encoding='utf-8')
c = csv.writer(csvfile)

c.writerow(['frame_n', 'sent', 'received', 'processed'])

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
  stream.close()

  processed = int(time() * 1000)
  print([frame, sent, received, processed])
  c.writerow([frame, sent, received, processed])