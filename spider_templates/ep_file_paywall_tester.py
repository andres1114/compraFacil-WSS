# -*- coding: UTF-8 -*-

import os
import re
import subprocess
import sys
import unicodedata
import time
import requests

# Get the output argument
if len(sys.argv) > 1:
    if sys.argv[2] is None:
        logOutputMode = 0
    else:
        logOutputMode = int(sys.argv[2])

    domainId = sys.argv[1]
else:
    logOutputMode = 0
    domainId = 0

if domainId == 0:
    err_message = "ERR_NO_DOMAIN_ID_GIVEN There was no domain id given, use the script as follows: python3.6 ep_file_paywall_tester.py int DOMAIN_ID int LOG_OUTPUT"
    sys.exit(err_message)


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
            db_connection.set_client_encoding("utf8")
        except Exception as e:
            errorMessage = "ERR_CANT_CREATE_DB_CONNECTION " + str(e)
            verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="ep_file_paywall_db_connection")
            sys.exit(errorMessage)
    elif db_type == 'postgres':
        try:
            import psycopg2
            db_connection = psycopg2.connect(host=db_host, user=db_user, password=db_pass, dbname=db_name)
            db_connection.set_client_encoding("utf8")
        except Exception as e:
            errorMessage = "ERR_CANT_CREATE_DB_CONNECTION " + str(e)
            verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="ep_file_paywall_db_connection")
            sys.exit(errorMessage)
    elif db_type == 'sqlite':
        try:
            import sqlite3
            db_connection = sqlite3.connect(db_host)
        except Exception as e:
            errorMessage = "ERR_CANT_CREATE_DB_CONNECTION " + str(e)
            verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="ep_file_paywall_db_connection")
            sys.exit(errorMessage)
    return db_connection


def dbConnectionExecuteQuery(**kwargs):
    try:
        cursor = kwargs.get('connectionObject').cursor()
    except Exception as e:
        errorMessage = "ERR_CANT_CREATE_DB_CONNECTION Caught exception '" + str(e) + "', on query reference '" + kwargs.get('queryReference') + "_CREATE_CURSOR'"
        verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="ep_file_paywall_db_connection")
        #sys.exit(errorMessage)
    try:
        if len(kwargs.get('queryArgs')) == 0:
            cursor.execute(kwargs.get('query'))
        else:
            cursor.execute(kwargs.get('query'), kwargs.get('queryArgs'))
    except Exception as e:
        errorMessage = "ERR_CANT_EXECUTE_CURSOR Caught exception '" + str(e) + "', on query reference '" + kwargs.get('queryReference') + "_EXECUTE_CURSOR'"
        verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="ep_file_paywall_db_connection")
        kwargs.get('connectionObject').rollback()
        #sys.exit(errorMessage)
    try:
        kwargs.get('connectionObject').commit()
    except Exception as e:
        errorMessage = "ERR_CANT_COMMIT_CURSOR Caught exception '" + str(e) + "', on query reference '" + kwargs.get('queryReference') + "_COMMIT_CURSOR'"
        verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="ep_file_paywall_db_connection")
        kwargs.get('connectionObject').rollback()
        #sys.exit(errorMessage)
    if cursor.description is None:
        fetchedResults = {}
        returnArray = [fetchedResults, 0, kwargs.get('query'), None]
    else:
        fetchedResults = cursor.fetchall()
        returnArray = [fetchedResults, len(fetchedResults), kwargs.get('query'), cursor.lastrowid]
    cursor.close()
    return returnArray

#Define the required constants
current_script_dir = os.path.dirname(os.path.abspath(__file__))
misc_database_filename = "misc_database.sqlite3"
log_database_filename = "epfile_log.sqlite3"
max_number_of_request_attempts = 15

proxy_host = "megaproxy.rotating.proxyrack.net"
proxy_port = "222"
proxy_auth = "eprensa:tecnicos"

verbose(outputMode=logOutputMode, outputMessage="Creating the database connections...", logName="ep_file_paywall")
db_connection_eprensa = createDbConnection(db_type='postgres', db_host='rwpgsql', db_user='tsg',db_pass='newPOST53', db_name='eprensa')
db_connection_epfilelog = createDbConnection(db_type='sqlite',db_host=re.sub(r"([^\/]+$)", "", current_script_dir) + log_database_filename, db_user='',db_pass='', db_name='')
db_connection_miscdb = createDbConnection(db_type='sqlite',db_host=re.sub(r"([^\/]+$)", "", current_script_dir) + misc_database_filename, db_user='',db_pass='', db_name='')
verbose(outputMode=logOutputMode, outputMessage="Done", logName="ep_file_paywall")

verbose(outputMode=logOutputMode, outputMessage="Getting the required domain data...", logName="ep_file_paywall")
#get the domain data
queryArgs = {"domain_id": domainId}
query = "SELECT "
query += "requieres_proxy, "                #pos 0
query += "is_pay_protected, "               #pos 1
query += "cookie_detection_node, "          #pos 2
query += "is_login_protected "              #pos 3
query += "FROM scrapy_spiders WHERE domain_id = %(domain_id)s"
queryData = dbConnectionExecuteQuery(connectionObject=db_connection_eprensa, query=query, queryArgs=queryArgs, queryReference="ep_file_paywall_query_01", errorOutputMode=logOutputMode)
verbose(outputMode=logOutputMode, outputMessage="Done", logName="ep_file_paywall")

domainNeedsProxy = queryData[0][0][0]
domainIsPayProtected = queryData[0][0][1]
domainIsLoginProtected = queryData[0][0][3]
if queryData[0][0][2] is not None:
    cookie_detection_node = str(unicodedata.normalize('NFKD', queryData[0][0][2]).encode('ascii', 'ignore').decode())
else:
    cookie_detection_node = ""

verbose(outputMode=logOutputMode, outputMessage="Getting the epfiles to process", logName="ep_file_paywall")
#Get the epfiles to process
query = "SELECT "
query += "id, "                     #pos 0
query += "ep_file_name, "           #pos 1
query += "is_in_process, "          #pos 2
query += "date_folder "             #pos 3
query += "FROM epfiles_queue WHERE is_in_process = '0'"

try_catch_flag = True
while try_catch_flag:
    try:
        queryData = dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs={},queryReference="ep_file_paywall_query_02",errorOutputMode=logOutputMode)
        try_catch_flag = False
    except Exception as e:
        try_catch_flag = True

verbose(outputMode=logOutputMode, outputMessage="Done", logName="ep_file_paywall")

if queryData[1] > 0:
    verbose(outputMode=logOutputMode, outputMessage="Found " + str(queryData[1]) + " ep_files to process", logName="ep_file_paywall")

    # loop through the epfiles to index them into ES
    for x in range(len(queryData[0])):
        queryArgs = (queryData[0][x][0],)
        query = "UPDATE epfiles_queue SET is_in_process = '1' WHERE id = ?"

        try_catch_flag = True
        while try_catch_flag:
            try:
                dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs,queryReference="ep_file_paywall_query_03", errorOutputMode=logOutputMode)
                try_catch_flag = False
            except Exception as e:
                try_catch_flag = True

        epfile_id = queryData[0][x][0]
        date_folder = queryData[0][x][3]
        epfile = queryData[0][x][1]

        epfile_path_to = current_script_dir + '/' + epfile

        if domainIsLoginProtected == True:
            verbose(outputMode=logOutputMode, outputMessage="The domain owner of this epfile is login protected", logName="ep_file_paywall")
            verbose(outputMode=logOutputMode, outputMessage="Opening the epfile...",logName="ep_file_paywall")
            try:
                epfile_handle_utf8 = open(epfile_path_to, "r", encoding="utf-8")
            except Exception as e:
                epfile_handle_utf8 = open(epfile_path_to, "r", encoding="ascii")
            verbose(outputMode=logOutputMode, outputMessage="Done", logName="ep_file_paywall")

            verbose(outputMode=logOutputMode, outputMessage="Getting the epfile's content...", logName="ep_file_paywall")
            try:
                epfile_content_utf8 = str(unicodedata.normalize('NFKD', epfile_handle_utf8.read()).encode('utf-8', 'ignore').decode())
            except Exception as e:
                epfile_content_utf8 = str(unicodedata.normalize('NFKD', epfile_handle_utf8.read()).encode('ascii', 'ignore').decode())
            verbose(outputMode=logOutputMode, outputMessage="Done", logName="ep_file_paywall")

            verbose(outputMode=logOutputMode, outputMessage="Closing the epfile", logName="ep_file_paywall")
            epfile_handle_utf8.close()

            verbose(outputMode=logOutputMode, outputMessage="Getting the URL from the epfile...",logName="ep_file_paywall")
            regex_pattern = re.compile(r"(?<=URL:).+(?=:URL)")
            regex_result = regex_pattern.search(epfile_content_utf8)
            ep_file_url = regex_result.group()
            verbose(outputMode=logOutputMode, outputMessage="Done, url found: '" + ep_file_url + "'",logName="ep_file_paywall")

            if domainNeedsProxy:
                verbose(outputMode=logOutputMode, outputMessage="Requesting the URL's HTML (with proxy)...",logName="ep_file_paywall")
                proxies = {
                    "https": "https://{}@{}:{}/".format(proxy_auth,proxy_host,proxy_port)
                }
            else:
                verbose(outputMode=logOutputMode, outputMessage="Requesting the URL's HTML...",logName="ep_file_paywall")
                proxies = {}

            headers = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Connection': 'keep-alive', 'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'cross-site', 'cache-control': 'no-cache',
            }
            ep_file_processed_flag = False
            attempt_counter = 0
            while ep_file_processed_flag == False:
                if attempt_counter <= max_number_of_request_attempts:
                    try:
                        request_response = requests.get(ep_file_url, headers=headers, proxies=proxies, verify=False)
                        ep_file_processed_flag = True
                        attempt_counter = 0
                    except Exception as e:
                        attempt_counter = (attempt_counter + 1)
                        verbose(outputMode=logOutputMode, outputMessage="The request failed, retrying...", logName="ep_file_paywall")
                        verbose(outputMode=logOutputMode, outputMessage="Attempt number: " + str(attempt_counter),logName="ep_file_paywall")
                        ep_file_processed_flag = False
                else:
                    ep_file_processed_flag = True
                    verbose(outputMode=logOutputMode, outputMessage="Max number of attempts exceded, skipping epfile with url " + ep_file_url,logName="ep_file_paywall")
                    continue

            verbose(outputMode=logOutputMode, outputMessage="Done", logName="ep_file_paywall")

            request_successful_flag = False
            if request_response.status_code == 200:
                request_successful_flag = True

                body_content_ascii = str(unicodedata.normalize('NFKD', request_response.text).encode('ascii', 'ignore').decode())
            else:
                verbose(outputMode=logOutputMode, outputMessage="The request returned HTTP code " + str(request_response.status_code),logName="ep_file_paywall")
                verbose(outputMode=logOutputMode, outputMessage="No further action will be taken",logName="ep_file_paywall")
                request_successful_flag = False
                body_content_ascii = ""

            if request_successful_flag == True:
                verbose(outputMode=logOutputMode, outputMessage="Checking for login nodes...", logName="ep_file_paywall")

                if cookie_detection_node != "" or cookie_detection_node is not None:
                    loginwall_detected = "false"

                    for node in cookie_detection_node.split("|"):
                        if node.strip() in body_content_ascii.strip():
                            loginwall_detected = "true"
                            verbose(outputMode=logOutputMode, outputMessage="The HTML's content has a matching login node, '" + node + "'", logName="ep_file_paywall")
                            break
                        else:
                            verbose(outputMode=logOutputMode, outputMessage="No login node found for '" + node + "'",logName="ep_file_paywall")
                else:
                    verbose(outputMode=logOutputMode, outputMessage="No login node found", logName="ep_file_paywall")
                    loginwall_detected = "false"

                if loginwall_detected == "true":
                    if domainIsPayProtected == True:
                        verbose(outputMode=logOutputMode, outputMessage="The domain owner of this epfile is payprotected", logName="ep_file_paywall")
                        verbose(outputMode=logOutputMode, outputMessage="Opening the epfile...", logName="ep_file_paywall")
                        try:
                            epfile_handle_utf8 = open(epfile_path_to, "w", encoding="utf-8")
                        except Exception as e:
                            epfile_handle_utf8 = open(epfile_path_to, "w", encoding="ascii")
                        verbose(outputMode=logOutputMode, outputMessage="Done", logName="ep_file_paywall")

                        verbose(outputMode=logOutputMode, outputMessage="Overwritting...",logName="ep_file_paywall")
                        epfile_content_utf8_edited = re.sub(r"Payprotected:false:Payprotected", "Payprotected:true:Payprotected", epfile_content_utf8)
                        epfile_handle_utf8.write(epfile_content_utf8_edited)
                        verbose(outputMode=logOutputMode, outputMessage="Done", logName="ep_file_paywall")

                        verbose(outputMode=logOutputMode, outputMessage="Closing the epfile", logName="ep_file_paywall")
                        epfile_handle_utf8.close()
                    else:
                        verbose(outputMode=logOutputMode, outputMessage="The domain owner of this epfile is not payprotected, no further action will be taken", logName="ep_file_paywall")

        verbose(outputMode=logOutputMode, outputMessage="Sending the ep_file to storage2", logName="ep_file_paywall")
        cmd = "rsync -ravc " + epfile_path_to + " storage2:/mnt/nfs/digital_news/" + str(date_folder) + "/ 2>&1"
        verbose(outputMode=logOutputMode, outputMessage="Used cmd: " + cmd, logName="ep_file_paywall")
        resp = subprocess.check_output(cmd, shell=True)
        verbose(outputMode=logOutputMode, outputMessage="storage2's response: " + str(resp), logName="ep_file_paywall")

        verbose(outputMode=logOutputMode,outputMessage="Executing repair mode for page grab to index the epfile...", logName="ep_file_paywall")
        cmd = 'ssh -t -o StrictHostKeyChecking=no tsg@epbatch011 "php /web/eprensa/scripts/page_grab/page_grab.php es repair ' + str(date_folder) + '/' + epfile + ' noforce 2>&1"'
        verbose(outputMode=logOutputMode, outputMessage="Used cmd: " + cmd, logName="ep_file_paywall")
        resp = subprocess.check_output(cmd, shell=True)
        verbose(outputMode=logOutputMode, outputMessage="epbatch011's response: " + str(resp), logName="ep_file_paywall")

        queryArgs = (epfile_id,)
        query = "DELETE FROM epfiles_queue WHERE id = ?"

        try_catch_flag = True
        while try_catch_flag:
            try:
                dbConnectionExecuteQuery(connectionObject=db_connection_miscdb, query=query, queryArgs=queryArgs,queryReference="ep_file_paywall_query_04", errorOutputMode=logOutputMode)
                try_catch_flag = False
            except Exception as e:
                try_catch_flag = True

        verbose(outputMode=logOutputMode, outputMessage="Deleting the temp ep_file", logName="ep_file_paywall")
        cmd = 'rm -rf ' + epfile_path_to
        verbose(outputMode=logOutputMode, outputMessage="Used cmd: " + cmd, logName="ep_file_paywall")
        subprocess.check_output(cmd, shell=True)

else:
    verbose(outputMode=logOutputMode, outputMessage="No epfiles found, exiting", logName="ep_file_paywall")

