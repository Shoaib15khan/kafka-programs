import json

from kafka import KafkaConsumer
consumer = KafkaConsumer('csvtopic',value_deserializer=lambda v: json.loads(v),auto_offset_reset='earliest',bootstrap_servers=['localhost:9092'])
for i in consumer:
    print(i.values)

# diction = {}
#
# diction["emplu"]=[]
# for i in consumer:
#     a=i.value
#     for v in a.items():
#         print(i.value)
#         print(diction)
#         diction["emplu"].append({"name":v[0],"phno":v[1]})
#         print(diction["emplu"].append({"name":v[0],"phno":v[1]}))
#         f = open("/home/tibil/js1.json", 'w')
#
#         json.dump(diction, f)


''''''

print("good")
