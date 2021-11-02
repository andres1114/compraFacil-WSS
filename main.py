import db_connection
import functions
import os
import os.path
import re
import subprocess
import sys
import time


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
current_dir_path = os.path.dirname(os.path.abspath(__file__))
spiders_directory_name = "spiders"
cookie_auto_update_scripts_directory_name = "auto_cookie_updater_scripts"
max_spiders_under_execution = 8
script_sleep_seconds = 20

functions.verbose(outputMode=logOutputMode, outputMessage="Checking whether main.py is already running...", logName="main")

shell_command = "ps -ef | grep 'main.py'| grep -v 'grep' | wc -l"
commandResponse = subprocess.check_output(shell_command, shell=True).decode("utf-8")

if int(commandResponse) > 2:
    functions.verbose(outputMode=logOutputMode, outputMessage="Script is already running, exiting ("+commandResponse+")",logName="main")
    sys.exit("SCRIPT_ALREADY_RUNNING")

functions.verbose(outputMode=logOutputMode, outputMessage="Creating the DB connection objects...", logName="main")
#Create the databas connection objects
db_connection_mysql = db_connection.createDbConnection(db_type='mysql', db_host='192.168.10.19', db_user='root', db_pass='admin', db_name='compraFacil')
functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="main")

functions.verbose(outputMode=logOutputMode, outputMessage="Checking if any spider's cookies can be auto-generated...", logName="main")
queryArgs = {}
query = "SELECT almacen.nombre_almacen FROM almacen INNER JOIN scrapy_spiders ON almacen.id = scrapy_spiders.domain_id WHERE active IS FALSE AND spider_status LIKE '%cookies have expired%'"
queryData = db_connection.dbConnectionExecuteQuery(connectionObject=db_connection_mysql, query=query, queryArgs=queryArgs, queryReference="main_01", errorOutputMode=logOutputMode)

if len(queryData[0]) > 0:
    functions.verbose(outputMode=logOutputMode, outputMessage="Done, found " + str(len(queryData[0])) + " spiders to auto-generate its cookies",logName="main")

    for x in range(len(queryData[0])):
        functions.verbose(outputMode=logOutputMode,outputMessage="Processing spider " + str(x + 1) + " of " + str(len(queryData[0])),logName="main")
        script_name = re.sub("[.]", "_", queryData[0][x][0]) + ".py"
        script_compelte_path_txt = current_dir_path + "/" + cookie_auto_update_scripts_directory_name + "/" + script_name

        if os.path.isfile(script_compelte_path_txt):
            functions.verbose(outputMode=logOutputMode, outputMessage="Executing the auto-cookie generator script...",logName="main")
            shell_command = "python3 " + script_compelte_path_txt
            functions.verbose(outputMode=logOutputMode, outputMessage="Command to use: '" + shell_command + "'",logName="main")
            commandResponse = subprocess.check_output(shell_command, shell=True)
            functions.verbose(outputMode=logOutputMode, outputMessage="Done, cmd's response:" + str(commandResponse),logName="main")
        else:
            functions.verbose(outputMode=logOutputMode,outputMessage="Script '" + script_compelte_path_txt + "' not found, skipping",logName="main")
else:
    functions.verbose(outputMode=logOutputMode, outputMessage="Done, no spiders to update its cookies found.",logName="main")

functions.verbose(outputMode=logOutputMode, outputMessage="Executing the cookie updater script...", logName="main")
shell_command = "php " + current_dir_path + "/" + "parse_cookie_json.php"
functions.verbose(outputMode=logOutputMode, outputMessage="Command to use: '" + shell_command + "'", logName="main")
commandResponse = subprocess.check_output(shell_command, shell=True)
functions.verbose(outputMode=logOutputMode, outputMessage="Done, cmd's response:" + str(commandResponse), logName="main")

#Get the spiders to execute
queryArgs = {}
functions.verbose(outputMode=logOutputMode, outputMessage="Getting the spiders to launch", logName="main")
query = "SELECT almacen.nombre_almacen FROM almacen INNER JOIN scrapy_spiders ON almacen.id = scrapy_spiders.domain_id WHERE scrapy_spiders.active IS TRUE"
queryData = db_connection.dbConnectionExecuteQuery(connectionObject=db_connection_mysql, query=query, queryArgs=queryArgs, queryReference="main_01", errorOutputMode=logOutputMode)

if len(queryData[0]) > 0:
    functions.verbose(outputMode=logOutputMode, outputMessage="Done, spiders found " + str(len(queryData[0])) + ", query used '" + queryData[2] + "'", logName="main")

    # Loop through the query results
    for x in range(len(queryData[0])):
        isProcessruning = False
        functions.verbose(outputMode=logOutputMode,  outputMessage="Processing spider " + str(x + 1) + " of " + str(len(queryData[0])), logName="main")

        #Get the spider's name
        spider_name = re.sub("[.]", "_", queryData[0][x][0])
        functions.verbose(outputMode=logOutputMode, outputMessage="Spider's name: " + spider_name, logName="main")
        functions.verbose(outputMode=logOutputMode,outputMessage="Checking whether the spider is already running...", logName="main")

        #Checking whether the spider is already running
        shell_command = "ps -ef"
        commandResponse = subprocess.check_output(shell_command, shell=True)

        if commandResponse.find(spider_name.encode()) is -1:
            isProcessruning = False
        else:
            isProcessruning = True

        functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="main")

        if isProcessruning:

            functions.verbose(outputMode=logOutputMode, outputMessage="Spider " + spider_name + " is already running, skipping", logName="main")
        else:
            functions.verbose(outputMode=logOutputMode,outputMessage="Checking whether the spider can be launched (max is " + str(max_spiders_under_execution) + ")...", logName="main")

            execute_spider = False
            while execute_spider == False:
                shell_command = "ps -ef | grep 'scrapy crawl'| grep -v 'grep' | wc -l"
                commandResponse = subprocess.check_output(shell_command, shell=True).decode("utf-8")

                functions.verbose(outputMode=logOutputMode,outputMessage="Current number of spiders in execution: " + commandResponse, logName="main")

                if int(commandResponse) >= max_spiders_under_execution:
                    functions.verbose(outputMode=logOutputMode,outputMessage="Waiting...",logName="main")
                    execute_spider = False
                    time.sleep(script_sleep_seconds)
                else:
                    execute_spider = True

            shell_command = "cd " + current_dir_path + "/" + spiders_directory_name + "/" + spider_name + "/; scrapy crawl " + spider_name + " -a log_output=" + logOutputArg + " &> /dev/null &"

            functions.verbose(outputMode=logOutputMode, outputMessage="Launching spider " + spider_name + " in directory '" + current_dir_path + "/" + spiders_directory_name + "'", logName="main")
            functions.verbose(outputMode=logOutputMode, outputMessage="Command to use: '" + shell_command + "'", logName="main")

            commandResponse = subprocess.check_output(shell_command, shell=True)
            functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="main")

else:
    functions.verbose(outputMode=logOutputMode, outputMessage="No spiders found", logName="main")

functions.verbose(outputMode=logOutputMode, outputMessage="Script done, exiting", logName="main")

