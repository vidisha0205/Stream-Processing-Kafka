from kafka import KafkaProducer
from time import sleep
import requests
import json
import pandas as pd
import requests
import time
import random
# import mariadb,sys
import mysql.connector
#mydb = mysql.connector.connect
cnx = mysql.connector.connect(
            user="root",
            password="",
            host="localhost",
            port=3306,
            database="wiki"

        )
cnx.autocommit = True
cursor = cnx.cursor()


S = requests.Session()

w_URL = "https://en.wikipedia.org/w/api.php"

PARAMS = {
    "format": "json",
    "rcprop": "title|ids|sizes|flags|user",
    "list": "recentchanges",
    "action": "query",
    "rclimit": "3"
}



# Producing as JSON
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
value_serializer=lambda m: json.dumps(m).encode('ascii'))

try:
    while(True):
            sleep(1)
            print("Data sent to consumer")
            R = S.get(url=w_URL,params = PARAMS)
            DATA = R.json()["query"]["recentchanges"]
            for i in DATA:
                print(i)
                page_id, type1, title, oldlen, newlen, user = i['pageid'], i['type'], i['title'], i['oldlen'], i['newlen'], i['user']
                cursor.execute("insert into pageinfo(page_id, type, title, oldlen, newlen, user) values(%s, %s,%s, %s, %s, %s)",(page_id, type1, title, oldlen, newlen, user))
                producer.send('wiki',i)
                producer.send('wiki-user',i)
                producer.send('wiki-pageid',i)

except:
    print("\nSTREAM ENDED. PERFORMING BATCH QUERIES")
    start = time.time()
    cursor.execute("SELECT TYPE,COUNT(TYPE) FROM PAGEINFO GROUP BY TYPE;")
    end = time.time()
    elapsed_time = end - start
    
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['Type', 'Count'])
    print(df)
    print("\nTime taken is "+str(round(elapsed_time,4))+"seconds")

