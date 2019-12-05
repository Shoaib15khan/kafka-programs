import csv
import json
from kafka import KafkaProducer

Fi = open("/home/tibil/d1.csv", "r")
Rs = csv.reader(Fi)
for i in Rs:
    pr = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode("utf-8"))
    pr.send('sendit', i)
