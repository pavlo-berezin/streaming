> pip install kafka-python
> pip install opencv-python
> kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic videoparts
> put any video under `video.mp4` in root.