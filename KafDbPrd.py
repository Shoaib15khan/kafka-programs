import json
import time

import mysql.connector as con
from kafka import KafkaProducer

db = con.connect(host="localhost",user="shoaib",passwd="Tibil123*",db="kafka")
cur=db.cursor()
cur.execute("select * from jss1")
sql=cur.fetchall()


print("tuple==",sql)
dictnr={}
dictnr=dict(sql)
print("dictnr==",dictnr)


pr=KafkaProducer(value_serializer=lambda m:json.dumps(m).encode("utf-8"))
pr.send("kftop1",dictnr)
time.sleep(1)
print("producing")