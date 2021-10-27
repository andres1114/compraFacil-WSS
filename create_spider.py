# -*- coding: UTF-8 -*-

import db_connection
import functions
import os
import subprocess
import re
import sys
import shutil
import unicodedata


# Get the output argument
if len(sys.argv) > 1:
    logOutputArg = sys.argv[1]
else:
    logOutputArg = "no_logs"

if logOutputArg == "save_logs":
    logOutputMode = 1
elif logOutputArg == "no_logs":
    logOutputMode = 0
else:
    logOutputArg = "no_logs"
    logOutputMode = 0

#Define the constants
spiders_directory_name = "spiders"
templates_directory_name = "spider_templates"
current_script_dir = os.path.dirname(os.path.abspath(__file__))
settings_file_name = "settings.py"
middleware_file_name = "middlewares.py"
context_file_name = "context.py"
spider_file_name = "spider.py"
ep_file_tester_filename = "ep_file_paywall_tester.py"
cookie_checker_filename = "check_domain_cookie.py"
sqlite_database_filename = "misc_database.sqlite3"
epfilelog_database_filename = "epfile_log.sqlite3"

temp_epfile_foldername = "temp_ep_files"

#Define the script funcions
def updateSpiderStatus(**kwargs):

    spider_id = kwargs.get("spider_id")
    spiderStatus = kwargs.get("status")
    spiderToupdateText = kwargs.get("statusText")

    if spiderStatus == 0:
        query = "UPDATE scrapy_spiders SET spider_status = %(status)s WHERE id = %(spider_id)s"
    else:
        query = "UPDATE scrapy_spiders SET spider_status = %(status)s, created_at = NOW() WHERE id = %(spider_id)s"

    queryArgs = {
        "spider_id": spider_id
        , "status": spiderToupdateText
    }
    queryData = db_connection.dbConnectionExecuteQuery(connectionObject=db_connection_mysql, query=query, queryArgs=queryArgs, queryReference="create_spider_02", errorOutputMode=logOutputMode)

functions.verbose(outputMode=logOutputMode, outputMessage="Creating the DB connection objects...", logName="create_spider")
#Create the databas connection objects
db_connection_mysql = db_connection.createDbConnection(db_type='mysql', db_host='192.168.10.13', db_user='root', db_pass='admin', db_name='compraFacil')

functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="create_spider")

functions.verbose(outputMode=logOutputMode, outputMessage="Checking whether spiders directory '" + current_script_dir + "/" + spiders_directory_name + "' exists...", logName="create_spider")
#Check whether the spider directory exists and create it if it doesn't
if not os.path.exists(current_script_dir + "/" + spiders_directory_name):
    functions.verbose(outputMode=logOutputMode, outputMessage="Directory does not exists, creating...", logName="create_spider")

    try:
        os.mkdir(current_script_dir + "/" + spiders_directory_name)
    except Exception as e:
        errorMessage = "ERR_CANT_CREATE_FOLDER Can not create folder '" + current_script_dir + "/" + spiders_directory_name + "' with error message '" + str(e) + "'"
        functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="db_connection")
        sys.exit(errorMessage)

functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="create_spider")

functions.verbose(outputMode=logOutputMode, outputMessage="Getting the domains to create spiders from...", logName="create_spider")
#Get the domains to create a spider from
queryArgs = {}
query = "SELECT "
query += "scrapy_spiders.domain_id"                 #pos 0
query += ",scrapy_spiders.allowed_url_segments"     #pos 1
query += ",scrapy_spiders.date_format"              #pos 2
query += ",scrapy_spiders.date_start_string"        #pos 3
query += ",scrapy_spiders.date_end_string"          #pos 4
query += ",scrapy_spiders.requires_cookie"          #pos 5
query += ",almacen.nombre_almacen"                  #pos 6
query += ",almacen.pagina_web_almacen"              #pos 7
query += ",scrapy_spiders.id "                      #pos 8
query += ",scrapy_spiders.date_locale "             #pos 9
query += ",scrapy_spiders.requieres_proxy "         #pos 10
query += ",scrapy_spiders.is_pay_protected "        #pos 11
query += ",scrapy_spiders.cookie_detection_node "   #pos 12
query += ",scrapy_spiders.is_login_protected "      #pos 13
query += "FROM scrapy_spiders "
query += "INNER JOIN almacen "
query += "ON scrapy_spiders.domain_id = domains.id "
query += "WHERE scrapy_spiders.created_at IS NULL"
queryData = db_connection.dbConnectionExecuteQuery(connectionObject=db_connection_mysql, query=query, queryArgs=queryArgs, queryReference="create_spider_01", errorOutputMode=logOutputMode)

#Check whether there is any domain to create a spider from
if len(queryData[0]) > 0:
    functions.verbose(outputMode=logOutputMode, outputMessage="Done, domains found " + str(len(queryData[0])) + ", query used '" + queryData[2] + "'", logName="create_spider")

    #Loop through the query results
    for x in range(len(queryData[0])):
        functions.verbose(outputMode=logOutputMode, outputMessage="Processing domains " + str(x + 1) + " of " + str(len(queryData[0])), logName="create_spider")

        #Completing the cokie checker cmd
        cookie_checker_cmd = "python3 " + current_script_dir + "/" + cookie_checker_filename

        #Create the spider's name
        spider_name_1 = re.sub("[.]", "_", queryData[0][x][6])
        spider_name_2 = queryData[0][x][6]
        domain_full_url = queryData[0][x][7]
        allowed_uri_segments = queryData[0][x][1]
        date_format = queryData[0][x][2]
        date_start_str = queryData[0][x][3]
        date_end_str = queryData[0][x][4]
        domain_cookie_flag = str(queryData[0][x][5])
        domainId = queryData[0][x][0]
        date_locale = queryData[0][x][9]
        requires_proxy = str(queryData[0][x][10])
        is_pay_protected = str(queryData[0][x][11])
        is_login_protected = str(queryData[0][x][13])
        if (queryData[0][x][12] is None):
            cookie_detection_node = ''
        else:
            cookie_detection_node = queryData[0][x][12]

        spider_email_recipants = '["polako_1114@hotmail.es"]'

        functions.verbose(outputMode=logOutputMode, outputMessage="Checking if the domains has the full URL already set...", logName="create_spider")

        if domain_full_url is None:
            errorMessage = "ERR_CANT_CREATE_SPIDER Can not create spider '" + spider_name_1 + "' as the domain '" + spider_name_2 + "' (" + str(queryData[0][x][0]) + ") does not have the full URL set in the field 'almacen.pagina_web_almacens'"
            functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="create_spider")
            updateSpiderStatus(spider_id=queryData[0][x][8], status=0, statusText=errorMessage)
            continue

        functions.verbose(outputMode=logOutputMode, outputMessage="Checking whether spider '" + spider_name_1 + "' already exists in directory '" + current_script_dir + "/" + spiders_directory_name + "'",logName="create_spider")
        #Check whether the spider already exists
        if not os.path.exists(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1):
            #Create the crapy crawler project
            functions.verbose(outputMode=logOutputMode, outputMessage="Creating crawler project...", logName="create_spider")
            commandResponse = subprocess.check_output("cd " + current_script_dir + "/" + spiders_directory_name + "/; scrapy startproject " + spider_name_1 + "; ", shell=True)
            functions.verbose(outputMode=logOutputMode, outputMessage=commandResponse, logName="create_spider")

            #Create the scrapy spider
            functions.verbose(outputMode=logOutputMode, outputMessage="Creating spider...", logName="create_spider")
            commandResponse = subprocess.check_output("cd " + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "; scrapy genspider spider " + spider_name_2 + " -t crawl;", shell=True)
            functions.verbose(outputMode=logOutputMode, outputMessage=commandResponse, logName="create_spider")

            #Change the spider's settings
            functions.verbose(outputMode=logOutputMode, outputMessage="Configuring file '" + settings_file_name + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "'", logName="create_spider")
            try:
                #Open the settings template, get it's contents and replace the requiered parameters
                temp_template_content = open(current_script_dir + "/" + templates_directory_name + "/" + settings_file_name + ".txt", 'r').read()
                temp_template_content = re.sub(r"\?domain_proxy_flag", requires_proxy, temp_template_content)
                temp_template_content = re.sub(r"\?spider_name_1", spider_name_1, temp_template_content)
                temp_template_content = re.sub(r"\?domain_cookie_flag", domain_cookie_flag, temp_template_content)

                #Open the settings file and write the template contents
                file = open(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/" + settings_file_name, 'a')
                file.write(temp_template_content)
                file.close()

            except Exception as e:
                errorMessage = "ERR_CANT_WRITE_FILE Can not write to file '" + settings_file_name + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 +  "' with error message " + str(e) + "'"
                functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="create_spider")
                updateSpiderStatus(spider_id=queryData[0][x][8], status=0, statusText=errorMessage)
                sys.exit(errorMessage)

            functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="create_spider")

            #Change the spider's middleware
            functions.verbose(outputMode=logOutputMode, outputMessage="Configuring file '" + middleware_file_name + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "'", logName="create_spider")
            try:
                # Open the middleware template, get it's contents and replace the requiered parameters
                temp_template_content = open(current_script_dir + "/" + templates_directory_name + "/" + middleware_file_name + ".txt", 'r').read()

                # Open the middleware file and write the template contents
                file = open(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/" + middleware_file_name, 'a')
                file.write(temp_template_content)
                file.close()

            except Exception as e:
                errorMessage = "ERR_CANT_WRITE_FILE Can not write to file '" + middleware_file_name + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "' with error message " + str(e) + "'"
                updateSpiderStatus(spider_id=queryData[0][x][8], status=0, statusText=errorMessage)
                functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="create_spider")
                sys.exit(errorMessage)

            functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="create_spider")

            #Create the spider's context
            functions.verbose(outputMode=logOutputMode, outputMessage="Creating file '" + context_file_name + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "'", logName="create_spider")
            try:
                # Open the context template, get it's contents and replace the requiered parameters
                temp_template_content = open(current_script_dir + "/" + templates_directory_name + "/" + context_file_name + ".txt", 'r').read()

                # Open the context file and write the template contents
                file = open(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/" + context_file_name, encoding='utf-8', mode='w+')
                file.write(temp_template_content)
                file.close()

            except Exception as e:
                errorMessage = "ERR_CANT_WRITE_FILE Can not write to file '" + context_file_name + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "' with error message " + str(e) + "'"
                functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="create_spider")
                updateSpiderStatus(spider_id=queryData[0][x][8], status=0, statusText=errorMessage)
                sys.exit(errorMessage)

            functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="create_spider")

            #Delete the spider's main code
            functions.verbose(outputMode=logOutputMode, outputMessage="Deleting file '" + spider_file_name + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders'", logName="create_spider")
            try:
                os.remove(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/" + spider_file_name)
            except Exception as e:
                errorMessage = "ERR_CANT_REMOVE_FILE Can not remove file '" + spider_file_name + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders' with error message " + str(e) + "'"
                functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="create_spider")
                updateSpiderStatus(spider_id=queryData[0][x][8], status=0, statusText=errorMessage)
                sys.exit(errorMessage)

            #Create the new spider's main code
            functions.verbose(outputMode=logOutputMode, outputMessage="Creating file '" + spider_name_1 + ".py' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders'", logName="create_spider")
            try:
                # Open the spider's main code template, get it's contents and replace the requiered parameters
                temp_template_content = open(current_script_dir + "/" + templates_directory_name + "/" + spider_file_name + ".txt", 'r').read()
                temp_template_content = re.sub(r"\?domainId", str(domainId), temp_template_content)
                temp_template_content = re.sub(r"\?spider_name_1", spider_name_1, temp_template_content)
                temp_template_content = re.sub(r"\?spider_name_2", spider_name_2, temp_template_content)
                temp_template_content = re.sub(r"\?allowed_uri_segments", allowed_uri_segments, temp_template_content)
                temp_template_content = re.sub(r"\?date_format", date_format, temp_template_content)
                temp_template_content = re.sub(r"\?date_start_str", date_start_str, temp_template_content)
                temp_template_content = re.sub(r"\?date_end_str", date_end_str, temp_template_content)
                temp_template_content = re.sub(r"\?domain_full_url", domain_full_url, temp_template_content)
                temp_template_content = re.sub(r"\?cookie_checker_cmd", cookie_checker_cmd, temp_template_content)
                temp_template_content = re.sub(r"\?domain_cookie_flag", domain_cookie_flag, temp_template_content)
                temp_template_content = re.sub(r"\?date_locale", date_locale, temp_template_content)
                temp_template_content = re.sub(r"\?spider_email_recipants", spider_email_recipants, temp_template_content)
                temp_template_content = re.sub(r"\?domain_proxy_flag", requires_proxy, temp_template_content)
                temp_template_content = re.sub(r"\?domain_login_protection_flag", is_login_protected, temp_template_content)
                temp_template_content = re.sub(r"\?cookie_detection_node", cookie_detection_node, temp_template_content)

                # Open the spider's main code file and write the template contents
                file = open(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/" + spider_name_1 + ".py", 'w+', encoding="utf-8")
                file.write(temp_template_content)
                file.close()

            except Exception as e:
                errorMessage = "ERR_CANT_WRITE_FILE Can not write to file '" + spider_name_1 + ".py' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders' with error message " + str(e) + "'"
                functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="create_spider")
                updateSpiderStatus(spider_id=queryData[0][x][8], status=0, statusText=errorMessage)
                sys.exit(errorMessage)
            # copy the misc database into the folder
            copy_src_path = current_script_dir + "/" + templates_directory_name + "/"
            copy_dst_path = current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/"
            functions.verbose(outputMode=logOutputMode, outputMessage="Copying database file '" + sqlite_database_filename + "' from directory '" + copy_src_path + " to directory '" + spiders_directory_name + "'", logName="create_spider")
            functions.verbose(outputMode=logOutputMode, outputMessage="Copying database file '" + epfilelog_database_filename + "' from directory '" + copy_src_path + " to directory '" + spiders_directory_name + "'", logName="create_spider")
            try:
                shutil.copy(copy_src_path + sqlite_database_filename, copy_dst_path + sqlite_database_filename)
                shutil.copy(copy_src_path + epfilelog_database_filename, copy_dst_path + epfilelog_database_filename)
            except Exception as e:
                errorMessage = "ERR_CANT_COPY_FILE Can not copy database file '" + sqlite_database_filename + "' to directory '" + copy_dst_path + "' with error message " + str(e) + "'"
                functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="create_spider")
                updateSpiderStatus(spider_id=queryData[0][x][8], status=0, statusText=errorMessage)
                sys.exit(errorMessage)

            functions.verbose(outputMode=logOutputMode, outputMessage="Creating directory '" + temp_epfile_foldername + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/'", logName="create_spider")
            try:
                os.mkdir(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/" + temp_epfile_foldername)
            except Exception as e:
                errorMessage = "ERR_CANT_CREATE_FOLDER Can not create folder '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/" + temp_epfile_foldername + "' with error message '" + str(
                    e) + "'"
                functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="db_connection")
                sys.exit(errorMessage)

            functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="create_spider")

            #Create the spider's ep_file tester
            functions.verbose(outputMode=logOutputMode, outputMessage="Creating file '" + ep_file_tester_filename + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/'", logName="create_spider")
            try:
                # Open the context template, get it's contents and replace the requiered parameters
                temp_template_content = open(current_script_dir + "/" + templates_directory_name + "/" + ep_file_tester_filename + ".txt", 'r').read()

                # Open the context file and write the template contents
                file = open(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/" + temp_epfile_foldername + "/" + ep_file_tester_filename, encoding='utf-8', mode='w+')
                file.write(temp_template_content)
                file.close()

            except Exception as e:
                errorMessage = "ERR_CANT_WRITE_FILE Can not write to file '" + ep_file_tester_filename + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/" + temp_epfile_foldername +"' with error message " + str(e) + "'"
                functions.verbose(outputMode=logOutputMode, outputMessage=errorMessage, logName="create_spider")
                updateSpiderStatus(spider_id=queryData[0][x][8], status=0, statusText=errorMessage)
                sys.exit(errorMessage)

            functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="create_spider")
            updateSpiderStatus(spider_id=queryData[0][x][8], status=1, statusText="Active")

        else:
            functions.verbose(outputMode=logOutputMode, outputMessage="Spider '" + spider_name_1 + "' already exists", logName="create_spider")
else:
    functions.verbose(outputMode=logOutputMode, outputMessage="Done, no domains found, query used '" + queryData[2] + "'", logName="create_spider")


