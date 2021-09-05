import os
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def verbose(**kwargs):

    if kwargs.get('outputMode') == 0:
        print("(" + time.strftime('%Y-%m-%d %H:%M:%S') + ") " + str(kwargs.get('outputMessage')))
    elif kwargs.get('outputMode') == 1:
        print("(" + time.strftime('%Y-%m-%d %H:%M:%S') + ") " + str(kwargs.get('outputMessage')))

        dir_path = os.path.dirname(os.path.abspath(__file__)) + "/"
        file = open(dir_path + kwargs.get('logName') + '.log', 'a')
        file.write("(" + time.strftime('%Y-%m-%d %H:%M:%S') + ") " + str(kwargs.get('outputMessage')) + "\n")
        file.close()

def send_email(**kwargs):

        # Define the SMTP email variables
        smtp_email_username = "demo@mailgun.eprensa.com"
        smtp_email_password = "Jwp3uByU"
        smtp_email_smtp_name = "smtp.mailgun.org"
        smtp_email_smtp_port = 25

        # Define the MIME object properties
        smtp_mime_object = MIMEMultipart('alternative')
        smtp_mime_object['Subject'] = kwargs.get("email_subject")
        smtp_mime_object['From'] = kwargs.get('email_from')
        smtp_mime_object['To'] = ', '.join(kwargs.get('email_to'))

        # Add the HTML body to the MIME object
        smtp_email_body_html = MIMEText(kwargs.get('email_html_message'), 'html')
        smtp_mime_object.attach(smtp_email_body_html)

        # Define the Email object
        smtp_email_object = smtplib.SMTP(smtp_email_smtp_name, smtp_email_smtp_port)
        smtp_email_object.ehlo()

        # Start the TTLS
        smtp_email_object.starttls()

        # Log into the mailing server
        smtp_email_object.login(smtp_email_username, smtp_email_password)

        # Send the email
        smtp_email_object.sendmail(kwargs.get('email_from'), kwargs.get('email_to'), smtp_mime_object.as_string())

        # Close the email connection
        smtp_email_object.quit()