


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

#cur.execute("UPDATE urlData SET englishText=''")
#con.commit()

cur.execute("SELECT url,plainText FROM urlData where englishText=''")# and domain not like 'http://www.khilafah.com%'")
results = cur.fetchall()


batchTranslateDelim = "\n<--------------------------------------------->\n"
specialChars = """ !@#$%^&*()_+-=[]\{}|;':",./<>? """

totalResults = len(results)

for i,result in enumerate(results[:]):

    print("Progress .... %f%% complete"%(100.0*float(i)/totalResults))

    #print("\n\n\n\n\n")
    url, text = result
    found = False
    toTranslate = []
    s = ""
    other = ""

    """

    Itterate over every character in the text.
    Try to convert every character to ascii. If a character doesn't
    convert, catch the exception, and we now know that is something we
    want to send to the translator. If we build a string of these characters
    until we hit one that does convert, and isn't punctuation etc. At that
    poin we are at the end of the arabic text, and can push it to a list.

    Once we have all the arabic (or at least non-ascii) collected, we submit it
    as a string to the translation server, and then put it back in the database
    int the englishText column.

    """

    print("extracting text ....%d characters"%len(text))
    for j,char in enumerate(text):
        try:
            other += char
            char.encode('ascii')
            if found and char in specialChars:
                s += char
            else:
                if found:
                    found = False
                    if all([ e in specialChars for e in s ]):
                        toTranslate.insert(s,0)
                    s=""
        except UnicodeEncodeError:
            if not found:
                found = True
                s = ""

            s += char

    if len(s):
        toTranslate.insert(s,0)

    print("Text extraction complete")

    if not len(toTranslate):
        continue

    print("Translating: ",url)
    print("Found %d elements to translate"%len(toTranslate))
    print("\n".join(toTranslate))
    
    req = requests.post("http://10.1.90.126:32773/translate", data={"text":batchTranslateDelim.join(toTranslate)})


    ret = json.loads( req.content )
    print("Translation complete")

    # Put data back in the databse
    cur.execute("UPDATE urlData set englishText=? WHERE url=?",
        (
            ret['translated_text'], url
        )
    )
    con.commit()
    print("URL complete\n\n")

con.close()
