import db_connection
import functions
import sys
import datetime
import os
import re
import json
import time


#Get the argument id
domain_id = sys.argv[1]

if not 'domain_id' in locals():
    errorMessage = "ERR_NO_ARGUMENT_FOUND the argument 'domain_id' was not found"
    functions.verbose(outputMode=0, outputMessage=errorMessage, logName="main")
    sys.exit(errorMessage)

# Get the output argument
if len(sys.argv) > 2:
    logOutputArg = sys.argv[2]
else:
    logOutputArg = "no_logs"

if logOutputArg == "save_logs":
    logOutputMode = 1
elif logOutputArg == "no_logs":
    logOutputMode = 0
else:
    logOutputArg = "no_logs"
    logOutputMode = 0


current_script_dir = os.path.dirname(os.path.abspath(__file__))
spiders_directory_name = "spiders"
cookie_file_filename = "cookies_holder.json"
email_sender = "polako_1114@hotmail.com"
email_recipients = ["polako_1114@hotmail.com"]

#Create the databas connection objects
db_connection_mysql = db_connection.createDbConnection(db_type='mysql', db_host='192.168.10.18', db_user='root', db_pass='admin', db_name='compraFacil')

# check the cookies status
queryArgs = {"domain": domain_id}
query = "SELECT scrapy_headers.id, scrapy_headers.domain_id, scrapy_headers.header_type, scrapy_headers.header_name, scrapy_headers.header_value, scrapy_headers.header_status,  scrapy_headers.created_at, scrapy_headers.active, scrapy_headers.days_until_expiration, almacen.nombre_almacen FROM scrapy_headers INNER JOIN almacen ON scrapy_headers.domain_id = almacen.id WHERE scrapy_headers.domain_id = %(domain)s AND scrapy_headers.header_type IN (1,3) AND scrapy_headers.header_status NOT LIKE '%%error%%' AND scrapy_headers.active IS TRUE"
queryData = db_connection.dbConnectionExecuteQuery(connectionObject=db_connection_mysql, query=query,queryArgs=queryArgs, queryReference="check_domain_cookie_01",errorOutputMode=logOutputMode)

# pos 0 = id
# pos 1 = domain_id
# pos 2 = header_type
# pos 3 = header_name
# pos 4 = header_value
# pos 5 = header_status
# pos 6 = created_at
# pos 7 = active
# pos 8 = days_until_expiration
# pos 9 = domain

# Check whether there is any value from the query
if len(queryData[0]) > 0:

    # define the spider directory name
    spider_name_1 = re.sub("[.]", "_", queryData[0][0][9])
    spider_name_2 = queryData[0][0][9]
    cookie_path_to = current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/" + cookie_file_filename
    cookie_dictionary = {}

    # Loop through the query values the check for expired values
    for x in range(len(queryData[0])):

        if queryData[0][x][8] is not None:

            days_to_check_cookie_expiration = queryData[0][x][8]
            expirationDate = (datetime.datetime.now() - datetime.timedelta(days=days_to_check_cookie_expiration))
        else:

            days_to_check_cookie_expiration = None
            expirationDate = datetime.datetime.now()

        if queryData[0][x][6] is not None:
            cookie_creation_date = queryData[0][x][6]
        else:
            cookie_creation_date = datetime.datetime.now()

        if cookie_creation_date < expirationDate and days_to_check_cookie_expiration != 0 and days_to_check_cookie_expiration is not None:
            # Update the scrapy spider
            queryArgs = {"domain_id": domain_id}
            query = "UPDATE scrapy_spiders SET spider_status = 'ERROR: cookies have expired', active = FALSE WHERE domain_id = %(domain_id)s"
            db_connection.dbConnectionExecuteQuery(connectionObject=db_connection_mysql, query=query,queryArgs=queryArgs, queryReference="check_domain_cookie_02",errorOutputMode=logOutputMode)

            # Update the scrapy header
            queryArgs = {"domain_id": domain_id}
            query = "UPDATE scrapy_headers SET header_status = 'ERROR: cookie has expired', active = FALSE WHERE domain_id = %(domain_id)s AND header_type = (1,3)"
            db_connection.dbConnectionExecuteQuery(connectionObject=db_connection_mysql, query=query,queryArgs=queryArgs, queryReference="check_domain_cookie_03",errorOutputMode=logOutputMode)

            # Remove the cookie from the spider folder
            try:
                os.remove(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/" + cookie_file_filename)
            except Exception as e:
                errorMessage = "ERR_CANT_REMOVE_FILE Can not remove file '" + cookie_file_filename + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders' with error message " + str(e) + "'"
                functions.verbose(outputMode=0, outputMessage=errorMessage, logName="create_spider")
                sys.exit(errorMessage)

            # Send the email to tell the user this cookie has expired
            # <email block>

            # Create the email subject
            email_subject = "Scrapy Cookie vencida (Login cookies) | " + str(spider_name_2) + " | " + str(domain_id)

            # Start the email body to be sent with the alert
            email_html_body = "<div style='font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,\"Helvetica Neue\",Arial,sans-serif,\"Apple Color Emoji\",\"Segoe UI Emoji\",\"Segoe UI Symbol\",\"Noto Color Emoji\"'>";

            email_html_body += "<h2>Se ha vencido las cookies</h2>"
            email_html_body += "<hr/>"
            email_html_body += "<p>Las cookies de inicio de sesión de la Scrapy spider " + str(spider_name_2) + " (" + str(spider_name_1) + ") han caducado</p>"
            email_html_body += "<p><b>Dominio:</b> " + str(spider_name_2) + "</p>"
            email_html_body += "<p><b>ID:</b> " + str(domain_id) + "</p>"
            email_html_body += "<br/>"
            email_html_body += "<p>La spider ha sido desabilitada y no se ejcutara mas hasta que la cookie sea actualizada</p>"

            #functions.send_email(email_subject=email_subject, email_from=email_sender, email_to=email_recipients,email_html_message=email_html_body)
            # </email block>

            print("0", end="")
            sys.exit()
        else:
            cookie_dictionary[queryData[0][x][3]] = queryData[0][x][4]

    # Check whether the cookies file has already been created
    if not os.path.exists(current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders/" + cookie_file_filename):
        # Write the cookie JSON file
        try:
            temp_content = open(cookie_path_to, 'a')
            temp_content.write(json.dumps(cookie_dictionary))
            temp_content.close()
        except Exception as e:
            errorMessage = "ERR_CANT_WRITE_FILE Can not write contents to file '" + cookie_file_filename + "' in directory '" + current_script_dir + "/" + spiders_directory_name + "/" + spider_name_1 + "/" + spider_name_1 + "/spiders' with error message " + str(e) + "'"
            functions.verbose(outputMode=0, outputMessage=errorMessage, logName="create_spider")
            sys.exit(errorMessage)

        print("1", end="")
    else:
        print("1", end="")
else:
    print("0", end="")