


import sqlite3 as db




class SpiderDatabase:


    def __init__(self, dbName="data.db"):
        self.__databaseConnection = db.connect(dbName)
        self.__dbCursor = self.__databaseConnection.cursor()

        self.createDatabaseTables()


    def createDatabaseTables(self):
        self.__dbCursor.execute(
            "CREATE TABLE if not exists urlData ( url text, plainText text, html text, domain text, englishText text DEFAULT '')"
        )

        self.__dbCursor.execute(
            "CREATE TABLE if not exists linkData ( domain text, sourcePage text, link text)"
        )

        self.__dbCursor.execute(
            "CREATE TABLE if not exists imageLinks ( domain text, sourcePage text, link text)"
        )

        self.__dbCursor.execute(
            "CREATE TABLE if not exists pdfLinks ( domain text, sourcePage text, link text)"
        )

        self.__dbCursor.execute(
            "CREATE TABLE if not exists errors ( domain text, error text, link text)"
        )

        self.__dbCursor.execute(
            "CREATE TABLE if not exists domains ( domain text, complete int DEFAULT 0 )"
        )

        self.__dbCursor.execute(
            "CREATE TABLE if not exists domainRedirect ( domain text, redirectTo text )"
        )

        self.__databaseConnection.commit()


    def addRedirect(self, domain, redirectTo):
        self.__dbCursor.execute(
            "INSERT OR IGNORE INTO domainRedirect(domain, redirectTo) VALUES(?, ?)",
            (
                domain,
                redirectTo,
            )
        )
        self.__databaseConnection.commit()

    def addError(self, domain, error, url):
        self.__dbCursor.execute(
            "INSERT OR IGNORE INTO errors(domain, error, link) VALUES(?, ?, ?)",
            (
                domain,
                error,
                url
            )
        )
        self.__databaseConnection.commit()


    def addDomain(self, domain):
        if not domain.startswith("http") and not domain.startswith("https"):
            domain = "http://"+domain
        # self.__rootDomain = domain

        # TODO: Shoulnd't have to do two calls
        self.__dbCursor.execute(
            "SELECT COUNT(*) FROM domains WHERE domain='%s'"%domain
        )
        results = self.__dbCursor.fetchall()

        if results[0] == 0:
            self.__dbCursor.execute(
                "INSERT OR IGNORE INTO domains(domain) VALUES(?)",
                (
                    domain,
                )
            )
            self.__databaseConnection.commit()
            self.addLink( domain, "", domain)


    def getIncompleteDomains(self):
        self.__dbCursor.execute(
            "SELECT * FROM domains WHERE complete=0"
        )
        results = self.__dbCursor.fetchall()
        return [ e[0] for e in results ]


    def getAllDomains(self):
        self.__dbCursor.execute(
            "SELECT * FROM domains"
        )
        results = self.__dbCursor.fetchall()
        return [ e[0] for e in results ]

    def checkDomain(self, domain):
        self.__dbCursor.execute(
            "SELECT count(*) FROM domains WHERE domain like '%?%'", domain
        )
        results = self.__dbCursor.fetchone()
        return results[0]

    def addLink(self, domain, url="", link=""):
        # This will allow adding multiple entries of different pages
        # linking to the same page. (many sourcePage, single link)
        self.__dbCursor.execute(
            "INSERT OR IGNORE INTO linkData(domain, sourcePage, link) VALUES(?, ?, ?)",
            (
                domain,
                url,
                link
            )
        )
        self.__databaseConnection.commit()

    def getLink(self, domain=""):
        self.__dbCursor.execute(
            "SELECT * from linkData WHERE domain LIKE '%%%s%%' LIMIT 1"%domain
        )
        results = self.__dbCursor.fetchone()
        # Doing the check as link like will remove all pointers to the page
        # as there could be multiple entries of different pages linking to it
        # self.__dbCursor.execute(
        #     "DELETE FROM linkData where link='%s'"%results[2]
        # )
        # self.__databaseConnection.commit()
        return results


    def removeLinkFromQueue(self, link):
        # Doing the check as link like will remove all pointers to the page
        # as there could be multiple entries of different pages linking to it
        self.__dbCursor.execute(
            "DELETE FROM linkData where link='%s'"%link
        )
        self.__databaseConnection.commit()


    def finishDomain(self, domain):
        self.__dbCursor.execute(
            "UPDATE domains SET complete='1' where domain='%s'"%domain
        )
        self.__databaseConnection.commit()

    def getNumberOfLinks(self, domain=""):
        # print(domain)
        self.__dbCursor.execute(
            "SELECT count(*) from linkData WHERE domain like '%%%s%%'"%domain

        )
        results = self.__dbCursor.fetchone()
        return results[0]

    def addImageLink(self, domain, url="", link=""):
        self.__dbCursor.execute(
            "INSERT OR IGNORE INTO imageLinks(domain, sourcePage, link) VALUES(?, ?, ?)",
            (
                domain,
                url,
                link
            )
        )
        self.__databaseConnection.commit()


    def addPDFLink(self, domain, url="", link=""):
        self.__dbCursor.execute(
            "INSERT OR IGNORE INTO pdfLinks(domain, sourcePage, link) VALUES(?, ?, ?)",
            (
                domain,
                url,
                link
            )
        )
        self.__databaseConnection.commit()

    def visitedLink(self, link):
        self.__dbCursor.execute(
            "SELECT count(*) FROM urlData WHERE url='%s'"%link
        )
        results = self.__dbCursor.fetchone()
        return results[0] > 0

    def insertPageData(self, domain, url, urlData, plainText):
        self.__dbCursor.execute(
            "INSERT INTO urlData(url, plainText, html, domain) VALUES(?, ?, ?, ?)",
            (
                url,
                plainText,
                # "".join([ str(e).strip() for e in bs.getText().split("\n") if len(str(e).strip()) ]),
                # str(urlData.decode('utf-8', "")),
                urlData,
                domain
            )
        )
        self.__databaseConnection.commit()
