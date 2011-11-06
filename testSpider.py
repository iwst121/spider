#!/usr/bin/env python2.7

"""Test system for the spider module.
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
"""

from spider import Spider
import logging
import argparse

parser = argparse.ArgumentParser(description="Test of threaded spider")
parser.add_argument('-t', '--threads', type=int, dest='threads',
                    default=3, help='Number of threads allowed')
parser.add_argument('-d', '--depth', type=int, dest='depth',
                    default=3, help='Maximum number of levels to spider')
parser.add_argument('url', metavar='URL')
args = parser.parse_args()

log = logging.getLogger(name="TestSpider")

def response (url, response):
    """A response has been recieved from the spider when contacting a url"""

    log.info("Processing %s" % url)

def htmlCallback(url, html):
    """Html has been found in the response"""

    log.info(html)

def linkCallback(parentUrl, linkUrl, element):
    """A link has been found in the page being spidered"""

    log.info("Link Found: %s %s %s" % (parentUrl, linkUrl, str(element)))

def levelCallback(levelNumber):
    """A new level of the page is being started"""
    
    log.info("Spidering level %d" % levelNumber)

if __name__ == '__main__':
    logging.basicConfig(level = logging.INFO,
                        format = "%(asctime)s %(levelname)s %(threadName)s: %(message)s")
    spider = Spider(args.url, threads=args.threads, maxDepth=args.depth)
    spider.responseCallback = response
    spider.htmlCallback = htmlCallback
    spider.linkCallback = linkCallback
    spider.levelCallback = levelCallback
    spider.spider()
