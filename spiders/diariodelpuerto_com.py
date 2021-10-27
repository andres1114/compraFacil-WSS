# -*- coding: UTF-8 -*-

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http import Request
from scrapy import signals
from scrapy.exceptions import CloseSpider
from datetime import datetime
from pytz import timezone
import subprocess
import unicodedata
from w3lib.http import basic_auth_header
import json
import os
import time
import locale
import sys
import random
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import urljoin

# Get the output argument
if len(sys.argv) > 3:
    logOutputArg = sys.argv[3].split("=")[1]
else:
    logOutputArg = "no_logs"

if logOutputArg == "save_logs":
    logOutputMode = 1
elif logOutputArg == "no_logs":
    logOutputMode = 0
else:
    logOutputArg = "no_logs"
    logOutputMode = 0

def verbose(**kwargs):
    if kwargs.get('outputMode') == 1:
        print("(" + time.strftime('%Y-%m-%d %H:%M:%S') + ") " + str(unicodedata.normalize('NFKD', kwargs.get('outputMessage')).encode('utf-8', 'ignore').decode()))
        
        dir_path = os.path.dirname(os.path.abspath(__file__)) + "/"
        file = open(dir_path + kwargs.get('logName') + '.log', 'a')
        file.write("(" + time.strftime('%Y-%m-%d %H:%M:%S') + ") " + str(unicodedata.normalize('NFKD', kwargs.get('outputMessage')).encode('utf-8', 'ignore').decode()) + "\n")
        file.close()

def createDbConnection(**kwargs):
    db_host = kwargs.get('db_host')
    db_user = kwargs.get('db_user')
    db_pass = kwargs.get('db_pass')
    db_name = kwargs.get('db_name')
    db_type = kwargs.get('db_type')
    if db_type == 'mysql':
        try:
            import MySQLdb
            db_connection = MySQLdb.connect(host=db_host, user=db_user, password=db_pass, dbname=db_name)
        except Exception as e:
            errorMessage = "ERR_CANT_CREATE_DB_CONNECTION " + str(e)
            verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage,logName="spider_db_connection")
            sys.exit(errorMessage)
    elif db_type == 'postgres':
        try:
            import psycopg2
            db_connection = psycopg2.connect(host=db_host, user=db_user, password=db_pass, dbname=db_name)
            db_connection.set_client_encoding("utf8")
        except Exception as e:
            errorMessage = "ERR_CANT_CREATE_DB_CONNECTION " + str(e)
            verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage,logName="spider_db_connection")
            sys.exit(errorMessage)
    elif db_type == 'sqlite':
        try:
            import sqlite3
            db_connection = sqlite3.connect(db_host)
        except Exception as e:
            errorMessage = "ERR_CANT_CREATE_DB_CONNECTION " + str(e)
            verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage,logName="spider_db_connection")
            sys.exit(errorMessage)
    return db_connection

def dbConnectionExecuteQuery(**kwargs):
    try:
        cursor = kwargs.get('connectionObject').cursor()
    except Exception as e:
        errorMessage = "ERR_CANT_CREATE_DB_CONNECTION Caught exception '" + str(e) + "', on query reference '" + kwargs.get('queryReference') + "_CREATE_CURSOR'"
        verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="spider_db_connection")
        sys.exit(errorMessage)
    try:
        if len(kwargs.get('queryArgs')) == 0:
            cursor.execute(kwargs.get('query'))
        else:
            cursor.execute(kwargs.get('query'), kwargs.get('queryArgs'))
    except Exception as e:
        errorMessage = "ERR_CANT_EXECUTE_CURSOR Caught exception '" + str(e) + "', on query reference '" + kwargs.get('queryReference') + "_EXECUTE_CURSOR'"
        verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="spider_db_connection")
        kwargs.get('connectionObject').rollback()
        sys.exit(errorMessage)
    try:
        kwargs.get('connectionObject').commit()
    except Exception as e:
        errorMessage = "ERR_CANT_COMMIT_CURSOR Caught exception '" + str(e) + "', on query reference '" + kwargs.get('queryReference') + "_COMMIT_CURSOR'"
        verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="spider_db_connection")
        kwargs.get('connectionObject').rollback()
        sys.exit(errorMessage)
    if cursor.description is None:
        fetchedResults = {}
        returnArray = [fetchedResults, 0, kwargs.get('query'), None]
    else:
        fetchedResults = cursor.fetchall()
        returnArray = [fetchedResults, len(fetchedResults), kwargs.get('query'), cursor.lastrowid]
    cursor.close()
    return returnArray

current_script_dir = os.path.dirname(os.path.abspath(__file__))
misc_database_filename = "misc_database.sqlite3"

db_connection_miscdb = createDbConnection(db_type='sqlite',db_host=current_script_dir + "/" + misc_database_filename, db_user='',db_pass='', db_name='')

spider_name = 'diariodelpuerto_com'
limitNumberOfExecutionsBeforeUpdatingSitemap = 200

close_spider_called = False

def update_spider_mode(**kwargs):
    verbose(outputMode=logOutputMode, outputMessage="Executing the update_spider_mode method...", logName="spider")
    if kwargs.get('update_mode') == True:
        verbose(outputMode=logOutputMode, outputMessage="Resetting the spider_execution_counter table to 1", logName="spider")
        query = "UPDATE spider_execution_counter SET value = 1"
        dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={}, queryReference="spider_query_01", errorOutputMode=logOutputMode)

        verbose(outputMode=logOutputMode, outputMessage="Setting the spider_execution_mode table to " + str(kwargs.get('update_value')),logName="spider")
        query = "UPDATE spider_execution_mode SET value = " + str(kwargs.get('update_value'))
        dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_02", errorOutputMode=logOutputMode)

        verbose(outputMode=logOutputMode, outputMessage="Deleting the urls in the log...", logName="spider")
        query = "DELETE FROM visited_urls"
        dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={}, queryReference="spider_query_03",  errorOutputMode=logOutputMode)
        verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")
    else:
        query = "SELECT value FROM spider_execution_counter LIMIT 0,1"
        queryData = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_04",errorOutputMode=logOutputMode)
        verbose(outputMode=logOutputMode,outputMessage="The current execution counter is now at " + str(queryData[0][0][0]) + " of " + str(limitNumberOfExecutionsBeforeUpdatingSitemap),logName="spider")

        if queryData[0][0][0] == limitNumberOfExecutionsBeforeUpdatingSitemap:
            verbose(outputMode=logOutputMode, outputMessage="Resetting the spider_execution_counter table to 1",logName="spider")
            query = "UPDATE spider_execution_counter SET value = 1"
            dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_05", errorOutputMode=logOutputMode)

            verbose(outputMode=logOutputMode,outputMessage="Setting the spider_execution_mode table to 1",logName="spider")
            query = "UPDATE spider_execution_mode SET value = 1"
            dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_06", errorOutputMode=logOutputMode)

            verbose(outputMode=logOutputMode, outputMessage="Deleting the urls in the log...", logName="spider")
            query = "DELETE FROM visited_urls"
            dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_07", errorOutputMode=logOutputMode)
            verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")
        else:
            verbose(outputMode=logOutputMode, outputMessage="Setting the spider_execution_counter table to " + str(queryData[0][0][0] + 1),logName="spider")
            query = "UPDATE spider_execution_counter SET value = " + str(queryData[0][0][0] + 1)
            dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_08", errorOutputMode=logOutputMode)

            verbose(outputMode=logOutputMode, outputMessage="Setting the spider_execution_mode table to 2", logName="spider")
            query = "UPDATE spider_execution_mode SET value = 2"
            dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_09", errorOutputMode=logOutputMode)


def get_spider_mode():
    query = "SELECT value FROM spider_execution_mode LIMIT 0,1"
    queryData = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_10", errorOutputMode=logOutputMode)
    return queryData[0][0][0]

def update_site_map():
    query = "UPDATE sitemap SET TTL = (TTL - 1) WHERE articles_found_in_run = 0"
    dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_11", errorOutputMode=logOutputMode)

    query = "DELETE FROM sitemap WHERE TTL <= 0"
    dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_12", errorOutputMode=logOutputMode)

    query = "UPDATE sitemap SET articles_found_in_run = 0"
    dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_13", errorOutputMode=logOutputMode)

def get_site_map():
    query = "SELECT url FROM sitemap ORDER BY TTL DESC"
    queryData = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_14", errorOutputMode=logOutputMode)

    if queryData[1] > 0:
        sitemap = []
        for x in range(len(queryData[0])):
            sitemap.append(queryData[0][x][0])

    else:
        errorMessage = "ERR_NO_SITEMAP_FOUND Caught exception, the query '" + query + "' returned 0 values, changing the spider execution mode and exiting"
        verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="spider")
        update_spider_mode(update_mode=True, update_value=1)
        sys.exit(errorMessage)

    return sitemap


class diariodelpuerto_comSpider(CrawlSpider):
    
    #Set the domain cookie flag
    domainNeedsProxy = True

    #Define the needed functions
    def clean_url(self, value):
        url = value
        cleanUrl = re.sub("#.*", "", url)
        cleanUrl = re.sub("[\/]$", "", cleanUrl)
        cleanUrl = re.sub("/\s/", "", cleanUrl)
        return cleanUrl

    def remove_url_from_visited_url_list(self,url):
        queryArgs = (url,)
        query = "DELETE FROM visited_urls WHERE url = ?"
        dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs,queryReference="spider_query_16", errorOutputMode=logOutputMode)
        queryArgs = {
            "url": url
        }
        query = "DELETE FROM dontgrablog WHERE url = %(url)s"
        dbConnectionExecuteQuery(connectionObject=db_connection_dontgrablog, query=query,queryArgs=queryArgs, queryReference="spider_query_17",errorOutputMode=logOutputMode)

    #Define the constants
    allowed_uri_segments = [5,6,7,8]
    to_match_node = '<div id=playeroptions'
    cookie_detection_node = str(unicodedata.normalize('NFKD', '').encode('ascii','ignore').decode())
    
    name = 'diariodelpuerto_com'
    allowed_domains = ['repelis24.me']
    domain_url = 'https://repelis24.me'

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Connection': 'keep-alive', 'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site', 'cache-control': 'no-cache',
        'Proxy-Authorization': basic_auth_header('eprensa', 'tecnicos')}
    proxyUrl = 'https://megaproxy.rotating.proxyrack.net:222'
    validUrls = 0

    spiderId = random.getrandbits(128)

    maxSitemapTTL = 100

    #Set the spider's settings
    custom_settings = {
        'CONCURRENT_REQUEST' : 1
    }

    spiderExecutionMode = get_spider_mode()

    if spiderExecutionMode == 1:
        verbose(outputMode=logOutputMode, outputMessage="Executing the spider on sitemap update mode", logName="spider")
        rules = [
            Rule(
                LinkExtractor(
                    unique=True
                ),
                follow=True,
                process_request='request_tagPage',
                callback="parse_items"
            )
        ]
        start_urls = [domain_url]
    elif spiderExecutionMode == 2:
        verbose(outputMode=logOutputMode, outputMessage="Executing the spider using the sitemap", logName="spider")
        rules = [
            Rule(
                LinkExtractor(
                    unique=True
                ),
                follow=False,
                process_request='request_tagPage',
                callback="parse_items"
            )
        ]
        start_urls = get_site_map()
    else:
        verbose(outputMode=logOutputMode, outputMessage="Executing the spider on sitemap update mode", logName="spider")
        rules = [
            Rule(
                LinkExtractor(
                    unique=True
                ),
                follow=True,
                process_request='request_tagPage',
                callback="parse_items"
            )
        ]
        start_urls = [domain_url]

    #Define the class functions
    @classmethod

    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(diariodelpuerto_comSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        verbose(outputMode=logOutputMode, outputMessage="The spider has finished scraping", logName="spider")
        verbose(outputMode=logOutputMode, outputMessage="Total valid urls: " + str(self.validUrls), logName="spider")

        if self.spiderExecutionMode == 1:
            verbose(outputMode=logOutputMode, outputMessage="Deleting the urls in the log...", logName="spider")
            query = "DELETE FROM visited_urls"
            dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={}, queryReference="spider_query_20",  errorOutputMode=logOutputMode)
            verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")

        query = "UPDATE visited_urls SET second_request_done = 0"
        dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={}, queryReference="spider_query_21", errorOutputMode=logOutputMode)

        update_spider_mode()
        update_site_map()

    def request_tagPage(self, request):
        newUrl = self.clean_url(request.url)
        verbose(outputMode=logOutputMode, outputMessage="Next url: " + newUrl, logName="spider")

        #Check whether the url to process has already been vissited
        verbose(outputMode=logOutputMode, outputMessage="Checking whether the url has already been visited", logName="spider")
        queryArgs = (newUrl,)
        query = "SELECT id FROM visited_urls WHERE url = ? AND second_request_done = 0"
        queryData_1 = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs, queryReference="spider_query_23",  errorOutputMode=logOutputMode)
        verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")

        if self.spiderExecutionMode == 1:
            # check whether the url is a defined section of the sitemap
            verbose(outputMode=logOutputMode, outputMessage="Checking whether the url is part of the sitemap",logName="spider")
            queryArgs = (newUrl,)
            query = "SELECT id FROM sitemap WHERE url = ?"
            queryData_2 = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs,queryReference="spider_query_24",errorOutputMode=logOutputMode)
            verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")

            if queryData_2[1] > 0:
                urlInSiteMap = True
            else:
                urlInSiteMap = False
        else:
            urlInSiteMap = False

        if queryData_1[1] > 0 or urlInSiteMap == True:
            verbose(outputMode=logOutputMode, outputMessage="The url has already been visited or is part of the sitemap, skipping", logName="spider")
            tagged = None
        else:
            if self.spiderExecutionMode == 2:
                queryArgs = (newUrl,)
                query = "SELECT id FROM sitemap WHERE url = ?"
                queryData = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs,queryReference="spider_query_25",errorOutputMode=logOutputMode)

                if queryData[1] > 0:
                    urlInSiteMap = True
                else:
                    urlInSiteMap = False
            else:
                urlInSiteMap = False

            if urlInSiteMap == True:
                verbose(outputMode=logOutputMode,outputMessage="The url is part of the sitemap and will not be added to the visited list", logName="spider")
            else:
                queryArgs = (newUrl,)
                query = "SELECT id FROM visited_urls WHERE url = ? AND second_request_done = 1"
                queryData_3 = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query,queryArgs=queryArgs, queryReference="spider_query_26",errorOutputMode=logOutputMode)

                if queryData_3[1] == 0:
                    verbose(outputMode=logOutputMode, outputMessage="The url has not been visited, adding to url visited list...", logName="spider")
                    queryArgs = (newUrl,)
                    query = "INSERT INTO visited_urls (url, date_visited, spider_id, second_request_done) VALUES (?, datetime('now'), " + str(self.spiderId) + ",0)"
                    dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs, queryReference="spider_query_27",  errorOutputMode=logOutputMode)
                    verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")
                else:
                    verbose(outputMode=logOutputMode, outputMessage="The url is in its second request cycle (with login cookies) and will not be added to the visited list", logName="spider")

            tagged = request.replace(url = newUrl)
            tagged = tagged.replace(headers = self.headers)
            #tagged.meta.update(cookiejar=random.randint(0,900000))
            tagged.meta.update(cookiejar=1)

            if self.domainNeedsProxy == True:
                tagged.meta["proxy"] = self.proxyUrl

        return tagged

    def parse_items(self, response):
        newUrl = self.clean_url(response.url)

        verbose(outputMode=logOutputMode, outputMessage="The current processed url is " + newUrl, logName="spider")
        uri_segments = newUrl.split('/')

        if len(uri_segments) in self.allowed_uri_segments:
            verbose(outputMode=logOutputMode, outputMessage="The url length is allowed, [" + str(len(uri_segments)) + "] in " + "[5,6,7,8]", logName="spider")

            body_text_ascii = str(unicodedata.normalize('NFKD', response.text).encode('ascii', 'ignore').decode())
            body_text_utf8 = str(unicodedata.normalize('NFKD', response.text).encode('utf-8', 'ignore').decode())

            if self.to_match_node.strip() in body_text_ascii.strip():
                valid_node = True
            else:
                valid_node = False

            if valid_node == False:
                verbose(outputMode=logOutputMode, outputMessage="This url doesn't have a match for " + self.to_match_node, logName="spider")
            else:
                verbose(outputMode=logOutputMode, outputMessage="Checking the url in the dont_grab_log table...", logName="spider")
                queryArgs = (newUrl,)
                query = 'SELECT url FROM movie_urls WHERE url = ?'
                queryData = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs, queryReference="spider_query_28",  errorOutputMode=logOutputMode)
                verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")

                if queryData[1] == 0:
                    verbose(outputMode=logOutputMode, outputMessage="The url has not been added to the table, adding", logName="spider")
                    queryArgs = (newUrl,)
                    query = 'INSERT INTO movie_urls (url) VALUES (?)'
                    queryData = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs, queryReference="spider_query_28", errorOutputMode=logOutputMode)
                    verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")

                else:
                    verbose(outputMode=logOutputMode, outputMessage="The url has already been added to the table, skipping", logName="spider")

                valid_url_found_in = self.clean_url(response.request.headers.get('Referer', None).decode('utf-8'))
                verbose(outputMode=logOutputMode, outputMessage="The valid url was found in " + valid_url_found_in, logName="spider")
                verbose(outputMode=logOutputMode, outputMessage="Checking whether the url has been saved in the sitemap table...", logName="spider")

                queryArgs = (valid_url_found_in,)
                query = "SELECT id, articles_found_in_run, TTL FROM sitemap WHERE url = ? LIMIT 0,1"
                queryData = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs,queryReference="spider_query_30",errorOutputMode=logOutputMode)
                verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")

                if queryData[1] == 0:
                    verbose(outputMode=logOutputMode, outputMessage="The url has not been added, adding it to the sitemap table...", logName="spider")
                    queryArgs = (valid_url_found_in,)
                    query = "INSERT INTO sitemap (url, articles_found_in_run, TTL) VALUES (?, 1, " + str(self.maxSitemapTTL) +")"
                    dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs,queryReference="spider_query_31",errorOutputMode=logOutputMode)
                    verbose(outputMode=logOutputMode, outputMessage="Done", logName="spider")

                else:
                    verbose(outputMode=logOutputMode, outputMessage="The url has already been added, skipping", logName="spider")

                    if queryData[0][0][1] == 0:
                        query = "UPDATE sitemap SET articles_found_in_run = 1 WHERE id = " + str(queryData[0][0][0])
                        dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_32",errorOutputMode=logOutputMode)

                    if queryData[0][0][2] < self.maxSitemapTTL:
                        query = "UPDATE sitemap SET TTL = (TTL + 10) WHERE id = " + str(queryData[0][0][0])
                        dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="spider_query_33",errorOutputMode=logOutputMode)

                self.validUrls = self.validUrls + 1
        else:
            verbose(outputMode=logOutputMode, outputMessage="The url length is not allowed, skipping", logName="spider")

