import logging
import smtplib
import sys
import traceback
from datetime import datetime
from email.mime.text import MIMEText
from credentials import *


# Configure logging
logging.basicConfig(filename='error.log', level=logging.ERROR)  # Log errors to a file named 'error.log'


# Function to handle errors and log them
def log_error(message):
    # Get current date and time
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
   
    exc_type, exc_value, exc_traceback = sys.exc_info()
    error_type = exc_type.__name__
    error_location = traceback.format_tb(exc_traceback)[-1]
    error_message = f"{current_time}: {error_type} occurred at {error_location}: {exc_value}"
   
    logging.error(error_message)
    print("Error occurred and logged - " + message, error_message)
    send_email("Error Notification: meeting dashboard", error_message)




def send_email(subject, body):
    gmail_user = GMAIL_USER  # Your Gmail address
    gmail_password = GMAIL_PASSWORD  # Your Gmail password
    gmail_recipient = GMAIL_RECIPIENT  # Your Gmail recipient


    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = gmail_user
    msg['To'] = gmail_recipient  # Recipient's email address


    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, gmail_recipient, msg.as_string())
        server.close()
        print('Email sent successfully')
    except Exception as e:
        print('Failed to send email:', e)