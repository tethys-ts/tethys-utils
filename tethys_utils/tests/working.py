# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 09:25:41 2019

@author: michaelek
"""
import smtplib
import ssl
import socket
import requests

#################################################
### Parameters

sender_address
sender_password
receiver_address = 'mullenkamp1@gmail.com'

subject = 'test'
body = 'test'


def email_msg(sender_address, sender_password, receiver_address, subject, body):
    """
    Function to send a simple email using gmail smtp.

    Parameters
    ----------
    sender_address : str
        The email address of the account that is sending the email.
    sender_password : str
        The password of the sender account.
    receiver_address : str or list of str
        The email addresses of the recipients.
    subject: str
        The subject of the email.
    body : str
        The main body of the email.

    Returns
    -------
    None

    """
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"

    msg_base = """Subject: {subject}\n
    {body}

    hostname: {host}
    IP address: {ip}"""

    ip = requests.get('https://api.ipify.org').text

    msg = msg_base.format(subject=subject, body=body, host=socket.getfqdn(), ip=ip)

    # Create a secure SSL context
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_address, sender_password)
        server.sendmail(sender_address, receiver_address, msg)


email_msg(sender_address, sender_password, receiver_address, subject, body)
