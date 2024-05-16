import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from credentials import *

# Set up the SMTP server
smtp_server = 'smtp.office365.com'
smtp_port = 587

# Your Office 365 email credentials
sender_email = EMAIL_USER
sender_password = EMAIL_PASSWORD

# Recipient email address
recipient_email = EMAIL_RECIPIENT

# Create a multipart message and set headers
message = MIMEMultipart()
message['From'] = sender_email
message['To'] = recipient_email
message['Subject'] = 'Python email test'

# Add body to email
body = 'Python testing body of the email'
message.attach(MIMEText(body, 'plain'))

# Connect to the SMTP server
server = smtplib.SMTP(smtp_server, smtp_port)
server.starttls()

# Login to your Office 365 account
server.login(sender_email, sender_password)

# Send email
server.sendmail(sender_email, recipient_email, message.as_string())

# Quit the server
server.quit()

print('Email sent successfully!')
