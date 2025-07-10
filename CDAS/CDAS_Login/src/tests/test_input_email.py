import unittest
from unittest.mock import patch

import os
import sys
sys.path.insert(0,'../../src')

from imbox import Imbox
import datetime

import logging
import logging.config
logging.config.fileConfig('logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)

from libCDAS.input_email import Email

class EmailTestCase(unittest.TestCase):
    
    def setUp(self):
        
        pass
    
    def tearDown(self):
        
        folder =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
        
        if os.path.exists(folder):
            os.rmdir(folder)
    
    def test_extract_msg_error(self):
        
        cred="config/email_login.json"
        folder =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
        
        if not os.path.isdir(folder):
            os.makedirs(folder, exist_ok=True)
        
        cdas_email = Email()
        username,password,host,sender=cdas_email.login_cred(cred) 
        mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)
        
        messages = mail.messages(subject='User Login Report',date__on=datetime.date.today(),sent_from=sender)
        
        expectederror = 'FileNotFoundError: [Errno 2] No such file or directory'
        
        with self.assertRaises(SystemExit) as cm, self.assertLogs('libCDAS.input_email', level='ERROR') as lc:
            fn1,n1 = cdas_email.extract_msg(mail,messages,'error_path')
        
        self.assertTrue(expectederror in lc.output[0])
        

    
    def test_download_rpt_error(self):
        
        cred="config/email_login.json"
        folder =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
        
        if not os.path.isdir(folder):
            os.makedirs(folder, exist_ok=True)
        
        cdas_email = Email()
        username,password,host,sender=cdas_email.login_cred(cred) 
        mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)
        
        expectederror = 'There is no Login Report today!'
        
        with self.assertRaises(SystemExit) as cm, self.assertLogs('libCDAS.input_email', level='ERROR') as lc:
            fn1,n1=cdas_email.download_rpt(folder,mail,'wrong@gmail.com')
        
        self.assertTrue(expectederror in lc.output[0])
    
if __name__ == '__main__':
    unittest.main()