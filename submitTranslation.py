


import sqlite3 as db
import requests
import json


con = db.connect("data.db")
cur = con.cursor()

cur.execute("SELECT * FROM urlData where englishText=''")
results = cur.fetchall()

for result in results:

    # cur.execute("SELECT count(*) FROM urlData WHERE url='%s' and domain='%s'"%
    #     (
    #     result[0],result[3]
    #     )
    # )
    # if cur.fetchone()[0] > 1:
    #     print(result[0], result[3])
    req = requests.post("http://10.1.90.126:32773/translate", data={"text":result[1]})


    ret = json.loads( req.content )


    cur.execute("UPDATE urlData set englishText='%s' WHERE url='%s' and domain='%s'"%
        (
            ret['translated_text'], result[0], result[3]
        )
    )
    con.commit()

con.close()
