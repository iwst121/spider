#!/usr/bin/env python

"""Threaded web spider module, spider.
Copyright (C) 2011 Derek Wisong

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

--------------------------------------------------------------------------------

This module provides the ability to traverse a web site to a specified number
of levels deep.  Callback functions may be provided to Spider instances in order
to add custom processing.

The output of a Spider is a SpiderResult instance containing data pertaining
to the site visited, including a graph of the layout of the site.

The bulk of time spent on the typical Spider application, is time spent waiting
for a web server to return some information in response to a query.  Therefore, 
Spider has the ability to be run multi-threaded, so that a single site can be
processed more rapidly.

Spider makes use of a ThreadPool class that I obtained from Emilio Monti at
http://code.activestate.com/recipes/577187-python-thread-pool/.
"""

__author__ = 'Derek Wisong'
__version__ = '1.0.0'

import ThreadPool
import urllib2
import threading
import lxml, lxml.html
import sys
import logging
from collections import deque

log = logging.getLogger(name='Spider')

## Get the mime type of an urllib2 response
def mime (response):
    """Get the mime type of an urllib2 response
    Parameters:
    response:  the urllib2 response

    Returns: The mime type, as a string
    """
    mimeType = response.info().getheader('content-type').split(';')[0]
    return mimeType

## Determine if the mime-type of a response is html
def ishtml(response):
    """Determine if the response to a http request is html, via mime type

    Parameters:
    response: urllib2 response

    Returns true if the mime type is html
    """
    mimeType = mime(response)
    return mimeType in ['text/html']

## Determine if the response is an image
def isimage(response):
    """Determine if the response to a http request is an image, via mime type
    
    Parameters:
    response: urllib2 response

    Returns true if the mime type is an image
    """
    imageTypes = ['image/gif', 'image/bmp', 'image/jpeg', 'image/png']
    return mime(response) in imageTypes


## results of a run of the spider
class SpiderResults(object):
    """Results of a run of a Spider instance.
    
    Properties:
    graph: adjacency list of pages on the website
    visited: dictionary of visited websites -> dictionary of info about the page
    """

    ## create a new instance of SpiderResults
    def __init__ (self):
        """Create a new instance of SpiderResults"""
        self.graph = {}
        self.visited = {}
        self._urls = deque()

## A multi-threaded web-spider supporing callbacks.
class Spider (object):
    """A multi-threaded web spider supporting callbacks.
    
    Properties
    ----------
    CALLBACKS: callables (function or class) with the below described parameters
    
        levelCallback -- Called when a level of the site is being started,
                         levelCallback(levelNumber), note levelNumber is 1-based
        linkCallback -- Called when a link is found, 
                        linkCallback(parentUrl, url, element)
        responseCallback -- Called upon connect, 
                            responseCallback(url, response)
        htmlCallback -- Called when html is parsed, 
                        htmlCallback(url, html)
    
    WARNING: When defining your callback functions, it is up to you to ensure
             that they are thread safe.  The Spider does not perform any sort
             of pre-made locking on callbacks.
    """

    ## Create a new Spider
    def __init__ (self, url, maxDepth=3, timeout=3, threads=3):
        """Create a Spider
        
        Parameters:
        url: the web address to start spidering from
        maxDepth: the maximum depth to spider through a site
        timeout: the max number of seconds before giving up on a connection
        threads: the number of threads to allow simultaneously
        """
        self.url = url
        self.maxDepth = maxDepth
        self.timeout = timeout
        self.threads = threads
        self.connectLock = threading.RLock()
        self.graphLock = threading.RLock()

        # initialize blank callbacks
        self.linkCallback = None
        self.responseCallback = None
        self.htmlCallback = None
        self.levelCallback = None

        # create thread pool
        self.threadPool = ThreadPool.ThreadPool(self.threads)
    
    ## get response from a website
    def _get (self, url, results):
        """Get a response from a website.
        
        Parameters:
        url: the url of the website to visit
        results: the SpiderResults instance the Spider is using
        
        Return the response to the http query
        """

        # While connecting, things like cookies may be modified, 
        # only allow 1 thread to connect simultaneously.
        self.connectLock.acquire()

        log.debug("Connecting to %s" % url)
        response = None
        isDead = False
        redirected = False
        contentType = ''
        redirectUrl = url
        contentEnc = ''
        exc = None

        try:
            response = urllib2.urlopen(url, timeout=self.timeout)
            redirectUrl = response.geturl()
            contentType = response.info().getheader('content-type')
            contentEnc = response.info().getheader('content-encoding')
            redirected = not url == redirectUrl
        except:
            excInfo = sys.exc_info()
            log.debug("Problem connecting: %s" % str(excInfo[0]))
            isDead = True
            exc = excInfo[0]
        finally:
            if url in results.visited:
                results.visited[url]['times'] += 1
                times = results.visited[url]['times']
            else:
                times = 1

            results.visited[url] = {'redirected':   redirected,
                                    'redirectUrl':  redirectUrl,
                                    'content-type': contentType,
                                    'content-enc':  contentEnc,
                                    'dead':         isDead,
                                    'exc':          exc,
                                    'times':        times}
            
            self.graphLock.acquire()
            if not isDead and not url in results.graph:
                results.graph[url] = []
            self.graphLock.release()
            
            self.connectLock.release()
            return response

    ## process a website, looking for links update graph
    def _processPage (self, url, results):
        """Process a website, looking for links to more, update graph
        
        Parameters:
        url: the url to process
        results: the SpiderResults instance
        """

        log.debug("Processing: %s" % url)
        response = self._get(url, results)
        if response is None: return
        if not self.responseCallback is None:
            self.responseCallback(url, response)

        if ishtml(response):
            html = lxml.html.parse(response).getroot()
            if html is None: return
            log.debug("Found html")
            html.make_links_absolute()
            if not self.htmlCallback is None:
                self.htmlCallback(url, html)
            
            for element, attribute, link, pos in html.iterlinks():
                self.graphLock.acquire()
                try:
                    if (attribute == 'href' and 
                        not link in results._urls and
                        not link in results.visited and
                        not link in results.graph[url]):
                        log.debug("Found link: %s" % link)
                        results._urls.append(link)
                        results.graph[url].append(link)
                        if not self.linkCallback is None:
                            self.linkCallback(url, link, element)
                finally:
                    self.graphLock.release()

    ## Spider the website
    def spider (self):
        """Spider the website.
        
        The spider processes each website using multiple threads until
        the maximum depth has been reached.

        Return a SpiderResults object containing a graph of the site and a
               list of information about visited pages
        """

        log.debug("Spidering %s" % self.url)
        results = SpiderResults()
        results.root = self.url

        # urls left to traverse
        if not self.levelCallback is None:
            self.levelCallback(1)

        self.threadPool.add_task(self._processPage, self.url, results)
        depth = 1

        while depth < self.maxDepth:
            self.threadPool.wait_completion()

            if not self.levelCallback is None:
                self.levelCallback(depth + 1)

            log.debug("Level: %d" % (depth + 1))

            for _ in range(len(results._urls)):
                url = results._urls.popleft()
                self.threadPool.add_task(self._processPage, url, results)

            depth += 1

        self.threadPool.wait_completion()
        log.debug("Spidering complete")
        return results
        

if __name__ == '__main__':
    s = Spider('http://www.google.com', threads=3, maxDepth=3)
    results = s.spider()

