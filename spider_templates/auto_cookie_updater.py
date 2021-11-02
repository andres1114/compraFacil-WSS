from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from pyvirtualdisplay import Display
import time
import datetime
import os
import re
import sys
import db_connection
import functions
import subprocess
import json
from fpdf import FPDF
from PIL import Image


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
wait_for_page_load = True
driver_sleep_seconds = 60
script_sleep_seconds = 10
current_script_dir = os.path.dirname(os.path.abspath(__file__))
main_directory_script = os.path.abspath(os.path.join(current_script_dir, '..'))
exntesions_path = current_script_dir + "/extensions/"
cookie_json_files_path = main_directory_script + "/cookie_json_files/"
driver_width = 1920
driver_height = 1080
driver_retry_counter = 0
driver_retry_limit = 100

#define the email info
email_sender = "info@eprensa.com"
email_recipients = ["apzapata@eprensa.com", "vduran@eprensa.com", "tclavell@eprensa.com"]

#this is the domain id, this value needs to be set by hand based on the domain that the auto-login will work on
domain_id = 0

functions.verbose(outputMode=logOutputMode, outputMessage="Starting the script for domain ID "+str(domain_id), logName="auto_cookie_updater_ID_"+str(domain_id))
functions.verbose(outputMode=logOutputMode, outputMessage="Creating the DB connection objects...", logName="auto_cookie_updater_ID_"+str(domain_id))
#Create the databas connection objects
db_connection_eppostgres = db_connection.createDbConnection(db_type='postgres', db_host='rwpgsql', db_user='tsg', db_pass='newPOST53', db_name='eprensa')
functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="auto_cookie_updater_ID_"+str(domain_id))

# Get the required values from the SQL
queryArgs = {
    "domain_id": domain_id
}
query = "SELECT scrapy_spiders.login_url, domains.src_url, scrapy_spiders.login_user, scrapy_spiders.login_password, domains.domain FROM scrapy_spiders INNER JOIN domains ON scrapy_spiders.domain_id = domains.id WHERE scrapy_spiders.domain_id = %(domain_id)s"
queryData = db_connection.dbConnectionExecuteQuery(connectionObject=db_connection_eppostgres, query=query, queryArgs=queryArgs, queryReference="auto_cookie_updater_ID_"+str(domain_id)+"_query_02", errorOutputMode=logOutputMode)

if len(queryData[0]) == 0:
    functions.verbose(outputMode=logOutputMode, outputMessage="There's no spider registered to domain id "+str(domain_id),logName="auto_cookie_updater_ID_" + str(domain_id))
    sys.exit()

domain_name = queryData[0][0][4]

# Define the user and password for Smarteca
website_username = queryData[0][0][2]
website_password = queryData[0][0][3]

# Define the website urls
website_url_1 = queryData[0][0][0]
website_url_2 = queryData[0][0][1]

functions.verbose(outputMode=logOutputMode, outputMessage="Creating the virtual display...", logName="auto_cookie_updater_ID_" + str(domain_id))
#Create the virtual display
display = Display(visible=0, size=(driver_width, driver_height))
display.start()
functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="auto_cookie_updater_ID_" + str(domain_id))

functions.verbose(outputMode=logOutputMode, outputMessage="Starting the web browser...", logName="auto_cookie_updater_ID_" + str(domain_id))
#Assign the arguments to the browser
options = webdriver.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
#options.add_argument('--headless')
options.add_argument('window-size=' + str(driver_width) + 'x' + str(driver_height))
options.add_extension(exntesions_path + 'cookie_agreement_extension_1.crx')
options.add_extension(exntesions_path + 'cookie_agreement_extension_2.crx')
#options.add_argument('--start-maximized')

#Start the browser
driver = webdriver.Chrome(chrome_options=options)
driver.set_window_size(driver_width, driver_height)
functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="auto_cookie_updater_ID_" + str(domain_id))

functions.verbose(outputMode=logOutputMode, outputMessage="Connecting to webpage '" + website_url_1 + "'...", logName="auto_cookie_updater_ID_" + str(domain_id))
#Get into the first url to log into wolterskluwer
driver.get(website_url_1)
functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="auto_cookie_updater_ID_" + str(domain_id))

functions.verbose(outputMode=logOutputMode, outputMessage="Accesing the webpage with the given username and password...", logName="auto_cookie_updater_ID_" + str(domain_id))

wait = WebDriverWait(driver, driver_sleep_seconds).until(
    expected_conditions.presence_of_element_located((By.ID, "Email1"))
)
#find the username input, and insert the username value
HTMLNode = driver.find_element_by_id("Email1")
HTMLNode.send_keys(website_username)

wait = WebDriverWait(driver, driver_sleep_seconds).until(
    expected_conditions.presence_of_element_located((By.ID, "Password"))
)
#find the password input, and insert the password value
HTMLNode = driver.find_element_by_id("Password")
HTMLNode.send_keys(website_password)

#Submit the login
HTMLNode.send_keys(Keys.RETURN)

functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="auto_cookie_updater_ID_" + str(domain_id))
time.sleep(script_sleep_seconds)

functions.verbose(outputMode=logOutputMode, outputMessage="Connecting to webpage '" + website_url_2 + "'...", logName="auto_cookie_updater_ID_" + str(domain_id))
#Open the home page
driver.get(website_url_2)
time.sleep(script_sleep_seconds)
functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="auto_cookie_updater_ID_" + str(domain_id))

functions.verbose(outputMode=logOutputMode, outputMessage="Getting the created cookies...", logName="auto_cookie_updater_ID_" + str(domain_id))
cookies = driver.get_cookies()
functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="auto_cookie_updater_ID_" + str(domain_id))

#Close the browser and the virtual display
driver.quit()
display.popen.terminate()

json_filename = str(domain_id)+'.json'
functions.verbose(outputMode=logOutputMode, outputMessage="Writting the cookies file " + json_filename + " into " + cookie_json_files_path +" ...", logName="auto_cookie_updater_ID_" + str(domain_id))
with open(cookie_json_files_path + json_filename, 'w', encoding='utf-8') as file:
    json.dump(cookies, file, ensure_ascii=False, indent=4)
functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="auto_cookie_updater_ID_" + str(domain_id))

#Send the email to inform that this cookie has been updated
# <email block>
# Create the email subject
email_subject = "Scrapy Cookie auto-generada | " + str(domain_name) + " | " + str(domain_id)

# Start the email body to be sent with the alert
email_html_body = "<div style='font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,\"Helvetica Neue\",Arial,sans-serif,\"Apple Color Emoji\",\"Segoe UI Emoji\",\"Segoe UI Symbol\",\"Noto Color Emoji\"'>";

email_html_body += "<h2>Se ha auto-generado las cookies</h2>"
email_html_body += "<hr/>"
email_html_body += "<p>Las cookies de la Scrapy spider " + str(domain_name) + " han sido auto-generadas exitosamente</p>"
email_html_body += "<p><b>Dominio:</b> " + str(domain_name) + "</p>"
email_html_body += "<p><b>ID:</b> " + str(domain_id) + "</p>"
email_html_body += "<br/>"
email_html_body += "<p>La spider sera habilitada nuevamente una vez el script 'parse_cookie_json.php' sea ejecutado y se actualizen las cookies</p>"

functions.verbose(outputMode=logOutputMode, outputMessage="Sending the email...", logName="auto_cookie_updater_ID_" + str(domain_id))
#functions.send_email(email_subject=email_subject, email_from=email_sender, email_to=email_recipients,email_html_message=email_html_body)
functions.verbose(outputMode=logOutputMode, outputMessage="Done", logName="auto_cookie_updater_ID_" + str(domain_id))
# <email block>

functions.verbose(outputMode=logOutputMode, outputMessage="Script done, exiting", logName="auto_cookie_updater_ID_" + str(domain_id))