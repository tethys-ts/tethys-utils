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

sender_address = 'noreply@tethys-ts.xyz'
sender_password = 'tethys-bot'
receiver_address = 'mgkittridge@gmail.com'
smtp_server = 'mail.tethys-ts.xyz'

subject = 'test'
body = 'test'




email_msg(sender_address, sender_password, receiver_address, subject, body, smtp_server)
