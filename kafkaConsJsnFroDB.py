import json

from kafka import KafkaConsumer
#to kafka consume we have to deserialize, auto reset offset is mandatory , (checkout the Speliing properly)
Consumer = KafkaConsumer('sendit',value_deserializer=lambda v: json.loads(v),auto_offset_reset='earliest',bootstrap_servers=['localhost:9092'])


# connection to Mysql
import mysql.connector
db = mysql.connector.connect(host="localhost",user="shoaib",passwd="Tibil123*",db="kafka")
cur=db.cursor() #places the cursor properly
cur.execute("create table if not exists jss1(name varchar(20), phno varchar(10))")
db.commit()   #  without commiting, cnt perfome any action on mysql side

global i  #just for iteration purpose "i" is used
i = 0
for message in Consumer:
    print(message)

    for k,v in message.value.items():
        while i < len(v):
            name = str(v[i]["name"])
            ph = str(v[i]["phone_no"])
            val = (name, ph)
            print(val)
            sql = 'insert into jss1 values("'+name+'","'+ph+'")'  #values directly inserting without using "%s"
            cur.execute(sql)
            i += 1
            db.commit()




print("over")


