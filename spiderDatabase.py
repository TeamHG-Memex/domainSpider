


# import sqlite3 as db
# import MySQLdb as db
import pymysql as db
import pymysql.cursors


class SpiderDatabase:


    def __init__(self, host, user, db,):
        # sqlite3
        # self._databaseConnection = db.connect(dbName)
        # self._dbCursor = self._databaseConnection.cursor()

        # MySQLdb
        # self._databaseConnection = db.connect("enterprise.local", user="hopperj", db="testing")
        # self._dbCursor = self._databaseConnection.cursor()

        # pymysql
        self._databaseConnection = db.connect(
            host="enterprise.local",
            user="hopperj",            
            db="testing"
        )
        self._dbCursor = self._databaseConnection.cursor()
        self.__destroyTables()
        self.__createDatabaseTables()

    def __destroyTables(self):
        try:
            self._dbCursor.execute(
                """DROP TABLE `domainRedirect`,
                `domains` ,
                `errors` ,
                `imageLinks` ,
                `linkData` ,
                `pdfLinks` ,
                `urlData` ;"""
            )
            self._databaseConnection.commit()
        except:
            print("Could not delete tables ....")

    def __createDatabaseTables(self):
        self._dbCursor.execute(
            "CREATE TABLE if not exists urlData ( url text, plainText text, html text, domain text, englishText text)"
        )

        self._dbCursor.execute(
            "CREATE TABLE if not exists linkData ( domain text, sourcePage text, link VARCHAR(255) UNIQUE)"
        )

        self._dbCursor.execute(
            "CREATE TABLE if not exists imageLinks ( domain text, sourcePage text, link text)"
        )

        self._dbCursor.execute(
            "CREATE TABLE if not exists pdfLinks ( domain text, sourcePage text, link text)"
        )

        self._dbCursor.execute(
            "CREATE TABLE if not exists errors ( domain text, error text, link text)"
        )

        self._dbCursor.execute(
            "CREATE TABLE if not exists domains ( domain VARCHAR(255) UNIQUE, complete int DEFAULT 0 )"
        )

        self._dbCursor.execute(
            "CREATE TABLE if not exists domainRedirect ( domain text, redirectTo VARCHAR(255) UNIQUE)"
        )

        self._databaseConnection.commit()
        print("Tables created")


    def addRedirect(self, domain, redirectTo):
        self._dbCursor.execute(
            "INSERT IGNORE INTO domainRedirect(domain, redirectTo) VALUES(%s, %s)",
            (
                domain,
                redirectTo,
            )
        )
        self._databaseConnection.commit()

    def addError(self, domain, error, url):
        print(domain, error, url)
        self._dbCursor.execute(
            "INSERT IGNORE INTO errors(domain, error, link) VALUES(%s, %s, `%s`)",
            (
                domain,
                error,
                url
            )
        )
        self._databaseConnection.commit()


    def addDomain(self, domain):
        if not domain.startswith("http") and not domain.startswith("https"):
            domain = "http://"+domain
        # self.__rootDomain = domain

        # TODO: Shoulnd't have to do two calls
        self._dbCursor.execute(
            "SELECT COUNT(*) FROM domains WHERE domain=%s",(domain,)
        )
        results = self._dbCursor.fetchone()
        print(" --> Add domain results:",results)
        if results[0] == 0:
            self._dbCursor.execute(
                'INSERT IGNORE INTO domains(domain) VALUES(%s)',
                (
                    domain,
                )
            )
            self._databaseConnection.commit()
            self.addLink( domain, "", domain)


    def getIncompleteDomains(self):
        self._dbCursor.execute(
            "SELECT * FROM domains WHERE complete=0"
        )
        results = self._dbCursor.fetchall()
        return [ e[0] for e in results ]


    def getAllDomains(self):
        self._dbCursor.execute(
            "SELECT * FROM domains"
        )
        results = self._dbCursor.fetchall()
        return [ e[0] for e in results ]

    def checkDomain(self, domain):
        self._dbCursor.execute(
            "SELECT count(*) FROM domains WHERE domain like %s", ('%'+domain+'%',)
        )
        results = self._dbCursor.fetchone()
        return results[0]

    def addLink(self, domain, url="", link=""):
        # This will allow adding multiple entries of different pages
        # linking to the same page. (many sourcePage, single link)
        self._dbCursor.execute(
            "INSERT IGNORE INTO linkData(domain, sourcePage, link) VALUES(%s, %s, %s)",
            (
                domain,
                url,
                link
            )
        )
        self._databaseConnection.commit()

    def getLink(self, domain=""):
        self._dbCursor.execute(
            "SELECT * from linkData WHERE domain LIKE %s LIMIT 1",('%'+domain+'%',)
        )
        results = self._dbCursor.fetchone()
        # Doing the check as link like will remove all pointers to the page
        # as there could be multiple entries of different pages linking to it
        # self._dbCursor.execute(
        #     "DELETE FROM linkData where link='%s'"%results[2]
        # )
        # self._databaseConnection.commit()
        return results


    def removeLinkFromQueue(self, link):
        # Doing the check as link like will remove all pointers to the page
        # as there could be multiple entries of different pages linking to it
        self._dbCursor.execute(
            "DELETE FROM linkData where link=%s",(link,)
        )
        self._databaseConnection.commit()


    def finishDomain(self, domain):
        self._dbCursor.execute(
            "UPDATE domains SET complete='1' where domain=%s",(domain,)
        )
        self._databaseConnection.commit()

    def getNumberOfLinks(self, domain=""):
        # print(domain)
        self._dbCursor.execute(
            "SELECT count(*) from linkData WHERE domain like %s",('%'+domain+'%',)

        )
        results = self._dbCursor.fetchone()
        return results[0]

    def addImageLink(self, domain, url="", link=""):
        self._dbCursor.execute(
            "INSERT IGNORE INTO imageLinks(domain, sourcePage, link) VALUES(%s, %s, %s)",
            (
                domain,
                url,
                link
            )
        )
        self._databaseConnection.commit()


    def addPDFLink(self, domain, url="", link=""):
        print(domain)
        print(url)
        print(link)
        self._dbCursor.execute(
            "INSERT IGNORE INTO pdfLinks(domain, sourcePage, link) VALUES(%s, %s, %s)",
            (
                domain,
                url,
                link.encode("utf-16")
            )
        )
        self._databaseConnection.commit()

    def visitedLink(self, link):
        self._dbCursor.execute(
            "SELECT count(*) FROM urlData WHERE url=%s",(link,)
        )
        results = self._dbCursor.fetchone()
        return results[0] > 0

    def insertPageData(self, domain, url, urlData, plainText):

        try:
            plainText = plainText.encode("utf-16", "ignore")
        except:
            plainText = ""

        try:
            urlData = urlData.encode("utf-16", "ignore")
        except:
            urlData = ""

        self._dbCursor.execute(
            "INSERT INTO urlData(url, plainText, html, domain) VALUES(%s, %s, %s, %s)",
            (
                url,
                plainText,
                urlData,
                domain
            )
        )
        self._databaseConnection.commit()
