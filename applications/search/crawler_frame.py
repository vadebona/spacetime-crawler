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

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

visitedLinks = set()

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

    for l in links:
        currLink = l['href']

        if rawDataObj.is_redirected:
            if "calendar" in rawDataObj.final_url:
                continue

        if rawDataObj.http_code == "404":
            continue

        if "#" in currLink:
            continue

        if "mailto" in currLink:
            continue

        if not is_absolute(currLink)[0]:
            currLink = baseURL + currLink

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

    if url in visitedLinks:
        return False

    parsed = urlparse(url)

    if parsed.scheme not in set(["http", "https"]):
        return False


    if "calendar" in parsed.path:
        if parsed.query:
            return False

    if parsed.netloc == "calendar.ics.uci.edu":
        return False

    if len(url) > 100:
        return False

    # duplicate directories in a link:
    parsedList = parsed.path.split("/")
    parsedSet = set()
    for pl in parsedList:
        if pl in parsedSet:
            return False
        parsedSet.add(pl)

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



if __name__ == '__main__':
    extract_next_links()
