


#import tika
from bs4 import BeautifulSoup
import sys
import traceback
import sqlite3 as db
import queue
from urllib.error import HTTPError, URLError
from urllib import request
from urllib import parse

import requests

from bs4 import UnicodeDammit
import time

from socket import gaierror

import spiderDatabase

class DomainSpider():

    def __init__(self, domain="", crawlerDelay=0.0):


        self.__currentDomain = domain

        self.__database = spiderDatabase.SpiderDatabase()

        self.__urlQueue = queue.Queue()
        self._visitedURLs = []

        self._hopCount = 0

        self.crawlerDelay = crawlerDelay



        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
           'Accept-Encoding': 'none',
           'Accept-Language': 'en-US,en;q=0.8',
           'Connection': 'keep-alive'
       }
    def addDomain(self, domain):
        self.__database.addDomain(domain)

    def crawl(self):
        url = "first URL"

        allDomains = self.__database.getAllDomains()

        for domain in self.__database.getIncompleteDomains():
            print("\n[+] Beginning domain:",domain)
            # self._hopCount = 0
            while self.__database.getNumberOfLinks(domain):# and self._hopCount<5:
                domain, source, url = self.__database.getLink(domain)
                print(
                    "    [*] Queue size: ",
                    self.__database.getNumberOfLinks(domain)
                )

                print("  [*] loading:",url)
                # req = request.Request(url, headers=self.headers)

                try:
                    # urlData = request.urlopen(req)
                    urlData = requests.get(url)

                    # If we tried to pull a domain, and got a redirect:
                    if ( url==domain ) and url != urlData.url:
                        self.__database.addRedirect(domain, urlData.url)
                        url = urlData.url
                        domain = urlData.url
                        allDomains.append( domain )
                        print("  [*] Added redirect to:",domain)

                    urlData = urlData.content
                except:
                    traceback.print_exc(file=sys.stdout)
                    self.__database.removeLinkFromQueue(url)
                    self.__database.addError(domain, "unknown error", url)
                    continue
                    # exit(1)
                #     if urlData.getcode() > 400:
                #         print("    [X] URL %d error"%urlData.getcode())
                #         self.__database.addError("url error %d"%urlData.getcode(), url)
                #         self.__database.removeLinkFromQueue(url)
                #         continue
                #
                #     # If we tried to pull a domain, and got a redirect:
                #     if ( url==domain ) and url != urlData.url:
                #         self.__database.addRedirect(domain, urlData.url)
                #         url = urlData.url
                #         domain = urlData.url
                #         allDomains.append( domain )
                #         print("  [*] Added redirect to:",domain)
                #
                # except (HTTPError, gaierror, URLError) as e:
                #     print("    [X] HTTPError")
                #     self.__database.addError(domain, str(e), url)
                #     self.__database.removeLinkFromQueue(url)
                #     continue
                #
                # if urlData.getcode() > 400:
                #     print("    [X] URL %d error"%urlData.getcode())
                #     self.__database.addError("url error %d"%urlData.getcode(), url)
                #     self.__database.removeLinkFromQueue(url)
                #     continue

                # urlData = urlData.read()
                print("    [*] URL Data read")
                bs = None
                try:
                    bs = BeautifulSoup(urlData, "html5lib")
                    print("    [*] BeautifulSoup parsed")

                    # Strip out code and stuff
                    for script in bs(["script", "style"]):
                        script.extract()
                except:
                    bs = BeautifulSoup("", "html5lib")
                    self.__database.addError(domain, "Could not parse BS data",url)
                    print("    [x] Error: BeautifulSoup could not parse")

                plainText = ""
                try:
                    plainText = '\n'.join(
                        [ e.strip() for e in bs.getText().split("\n") if len(e.strip()) ]
                    )
                except UnicodeDecodeError as e:
                    traceback.print_exc(file=sys.stdout)
                    self.__database.addError(domain, str(e),url)
                    print("\n    --> error!! <--\n")
                except:
                    traceback.print_exc(file=sys.stdout)
                    self.__database.addError(domain, "An unknown error while reading BS lines",url)

                print("    [*] Saving data ....")
                # Always dump whatever data we got.
                self.__database.insertPageData(domain, url, urlData, plainText)

                print("    [*] Dealing with links")
                # Do all the link stuff
                for link in bs.findAll("a", href=True):

                    # Don't follow intrapage links
                    if link['href'].startswith("#") or link['href'].endswith(".mp3") or link['href'].endswith(".avi"):
                        continue
                    # Make sure it is a full link, and lower case
                    link['href'] = parse.urljoin(url, link['href'])#.lower()

                    # Make sure it is prefaced with http/https
                    if not link['href'].startswith("http") and not link['href'].startswith("https"):
                        link['href'] = "http://"+link['href']


                    # TODO: strip away urls ending with #something

                    # this part needs work. Sometimes the trailing / is needed
                    # if link['href'].endswith("/"):
                    #     link['href'] = link['href'][:-1]

                    # Check if it is a link to an image
                    if any([ link['href'].endswith(e) for e in [".jpg", ".png", ".peg", ".gif", '.tff']] ):
                        self.__database.addImageLink( domain, url, link['href'] )


                    # Check if it is a PDF link
                    elif link['href'].endswith(".pdf"):
                        self.__database.addPDFLink( domain, url, link['href'])

                    # Must be a regular page link
                    else:
                        # Make sure we haven't already visited this page
                        if  any( e in link['href'] for e in allDomains )  and not self.__database.visitedLink( link['href'] ) and url != link['href']:
                            self.__database.addLink( domain, url, link['href'] )



                self.__database.removeLinkFromQueue(url)
                self._hopCount += 1
                time.sleep( self.crawlerDelay )
            # Flag the domain as being complete
            self.__database.finishDomain(domain)
            print("  [O] Completed domain:",domain)



if __name__ == "__main__":

    ds = DomainSpider()
    with open("websiteOutput.txt", 'r') as f:
    # with open("fakeWebsites.txt", 'r') as f:
        websites = f.readlines()
        for website in websites[:]:
            # print("\n\n\n****************************************************")
            # print("Website: ",website,"\n")
            ds.addDomain(website.strip())
        ds.crawl()
            # print("\n****************************************************")
