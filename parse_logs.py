import csv
import sys
import glob

BYTES_IN_MBPS = 125000

latency = 0
sum_bytes = 0
start_time = sys.maxsize
end_time = 0

for filename in glob.glob('logs/*.csv'):
  print('Parsing', filename)
  csvfile = open(filename, 'r', newline='', encoding='utf-8')

  reader = csv.reader(csvfile)

  next(reader)

  for row in reader:
    [frame, sent, received, processed, size] = row
    latency = max(int(processed) - int(sent), latency)
    sum_bytes = sum_bytes + int(size)
    start_time = min(start_time, int(received))
    end_time = max(end_time, int(processed))

throutput = sum_bytes / ((end_time - start_time) / 1000) / BYTES_IN_MBPS

print('Latency: {}ms'.format(latency))
print('Throutput: {} Mbps'.format('{0:.2f}'.format(throutput)))
