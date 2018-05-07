import logging
from datamodel.search.VadebonaAdesanyoKdmonten_datamodel import VadebonaAdesanyoKdmontenLink, OneVadebonaAdesanyoKdmontenUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
from bs4 import BeautifulSoup
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs
from uuid import uuid4
import requests, urllib2
import re

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

visitedLinks = set()
subdomains_count={}
max_page_links=('',0)
Invalid_Links=set()
crawler_traps=set()

@Producer(VadebonaAdesanyoKdmontenLink)
@GetterSetter(OneVadebonaAdesanyoKdmontenUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "VadebonaAdesanyoKdmonten"

    def __init__(self, frame):
        self.app_id = "VadebonaAdesanyoKdmonten"
        self.frame = frame


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OneVadebonaAdesanyoKdmontenUnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = VadebonaAdesanyoKdmontenLink("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get_new(OneVadebonaAdesanyoKdmontenUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(VadebonaAdesanyoKdmontenLink(l))

    def shutdown(self):
        write_Analytics()
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")

def is_absolute(link):
    if link[0:4]=='www.':
        link='http://'+link
    status=bool(urlparse(link).netloc)
    return (status,link)

def extract_next_links(rawDataObj):
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''
    outputLinks = set()

    s = BeautifulSoup(rawDataObj.content, 'lxml')
    links = s.find_all("a", href=True)
    baseURL = "http://" + urlparse(rawDataObj.url).netloc # netloc gets the domain url
    print "baseURL: ", baseURL

    global subdomains_count
    global max_page_links
    global crawler_traps
    global Invalid_Links
    global visitedLinks

    linksCount=len(links)
    subdomains_count[rawDataObj.url]=linksCount
    subdomains_report(rawDataObj.url,linksCount)
    if linksCount >  max_page_links[1]:
        max_page_links=(rawDataObj.url,linksCount)
        most_out_links_report(rawDataObj.url,linksCount)

    for l in links:
        currLink = l['href']

        if rawDataObj.is_redirected:
            if "calendar" in rawDataObj.final_url:
                crawler_traps.add(currLink)
                Invalid_Crawler_Trap(currLink,"Crawler Trap calendar")
            continue

        if rawDataObj.http_code == "404":
            Invalid_Links.add(currLink)
            Invalid_Crawler_Trap(currLink,"Invalid Link: 404")
            continue

        if "#" in currLink:
            Invalid_Links.add(currLink)
            Invalid_Crawler_Trap(currLink,"Invalid Link")
            continue

        if "mailto" in currLink:
            Invalid_Links.add(currLink)
            Invalid_Crawler_Trap(currLink,"Invalid Link: Mail")
            continue

        if not is_absolute(currLink)[0]:
            currLink = baseURL + currLink

        if is_absolute(currLink)[0]==False:
            Invalid_Links.add(currLink)
            Invalid_Crawler_Trap(currLink,"Invalid Link: Not Absolute")

            continue

        outputLinks.add(currLink)

    return list(outputLinks)

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    ## CRAWLER TRAP CHECKS:
    # [x] calendar.ics.uci.edu is filtered
    # [x] Length does not exceed 100 characters
    # [x] Not already in visitedLinks set
    # [x] finding duplicate directories in a link

    global subdomains_count
    global max_page_links
    global crawler_traps
    global Invalid_Links
    global visitedLinks
    
    if url in visitedLinks:
        Invalid_Crawler_Trap(url,"Crawler Trap Duplicate url")
        return False

    parsed = urlparse(url)

    if parsed.scheme not in set(["http", "https"]):
        return False


    if "calendar" in parsed.path:
        if parsed.query:
            crawler_traps.add(url)
            Invalid_Crawler_Trap(url,"Crawler Trap calendar")
            return False

    if parsed.netloc == "calendar.ics.uci.edu":
        crawler_traps.add(url)
        Invalid_Crawler_Trap(url,"Crawler Trap contains calendar")
        return False

    if len(url) > 100:
        crawler_traps.add(url)
        Invalid_Links.add(url)
        Invalid_Crawler_Trap(url,"Crawler Trap url len > 100")
        return False

    # duplicate directories in a link:
    '''
    parsedList = parsed.path.split("/")
    for pl in parsedList:
        if parsedList.count(pl)>1:
            crawler_traps.add(url)
            Invalid_Crawler_Trap(url,"Crawler Trap Duplicate Directories")
            return False
    '''
    try:
        if ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower()):
            print url
            visitedLinks.add(url)
            return True
        else:
            return False

    except TypeError:
        print ("TypeError for ", parsed)
        return False

def subdomains_report(link,count):
    file = open("Analytics_subdomains_report.txt","a")
    file.write("{} : {}\n".format(link,count))
    file.close()

def most_out_links_report(link,count):
    file = open("Analytics_most_out_links_report.txt","w")
    file.write("{} : {}\n".format(link,count))
    file.close()

def Invalid_Crawler_Trap(link,label):
    file = open("Analytics_Invalid_CrawlerTraps_report.txt","a")
    file.write("{} : {}\n".format(link,label))
    file.close()

def write_Analytics():
    global subdomains_count
    global max_page_links
    global crawler_traps
    global Invalid_Links
    global visitedLinks
    
    file = open("Analytics.txt","w")
    file.write("Base URL: Link Count")
    for key,value in subdomains_count.items():
        file.write("{} : {}".format(key,value))
    file.write("----------------------------------------------------------------------------")
    file.write("Invalid Links")
    file.write("----------------------------------------------------------------------------")
    for link in Invalid_Links:
        file.write(link)
    file.write("----------------------------------------------------------------------------")
    file.write("Crawler Traps")
    file.write("----------------------------------------------------------------------------")
    for link in crawler_traps:
        file.write(link)
    file.close()



