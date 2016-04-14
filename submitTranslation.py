


import sqlite3 as db
import requests
import json
import re

"""

A quick and dirty script that will:
 - retrieve data from the database file
 - find entries that need translation
 - breaks apart the non-english
 - translates it
 - puts the data back in the database into the englishText column

"""

# http://www.khilafah.com/

con = db.connect("data.db")
cur = con.cursor()

cur.execute("UPDATE INTO urlData SET englishText=''")
con.commit()

cur.execute("SELECT url,plainText FROM urlData where englishText=''")# and domain not like 'http://www.khilafah.com%'")
results = cur.fetchall()


batchTranslateDelim = "\n<--------------------------------------------->\n"
specialChars = """ !@#$%^&*()_+-=[]\{}|;':",./<>? """

for i,result in enumerate(results[:]):
    print("\n\n\n\n\n")
    url, text = result
    found = False
    toTranslate = []
    s = ""
    other = ""
    for j,char in enumerate(text):
        try:
            other += char
            char.encode('ascii')
            if found and char in specialChars:
                s += char
            else:
                if found:
                    found = False
                    # print("COMPLETE: ",s)
                    toTranslate.append(s)
        except UnicodeEncodeError:
            if not found:
                found = True
                s = ""

            s += char
    # print("COMPLETE: ",s)
    # print("\n\nOTHER",other)


    req = requests.post("http://10.1.90.126:32773/translate", data={"text":batchTranslateDelim.join(toTranslate)})


    ret = json.loads( req.content )

    # Put data back in the databse
    cur.execute("UPDATE urlData set englishText='%s' WHERE url='%s' and domain='%s'"%
        (
            ret['translated_text'], result[0], result[3]
        )
    )
    con.commit()

con.close()
