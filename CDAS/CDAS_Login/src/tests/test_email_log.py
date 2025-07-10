import unittest
from unittest.mock import patch
import datetime
import mariadb

import sys
import os
sys.path.insert(0,'../../src')

from libEmailLog.email_log import run,EmailLog,LogFile

class EmailLogTestCase(unittest.TestCase):
    
    def setUp(self):
        pass
        
    def tearDown(self):
        pass
        
    def test_email_login_fail(self):
        email = EmailLog()
        email.email_server = 'wongzhenyang.com' # error in email server
        expectederror = "ERROR:libEmailLog.email_log:Fail to Connect to Email Server. Error Code: 535. Error Message: b'5.7.8 Username and Password not accepted."
        with self.assertLogs('libEmailLog.email_log', level='ERROR') as lc:
            stmp = email.login()
        self.assertTrue(expectederror in lc.output[0])
    
    def test_email_message_with_image_attachment(self):
        email = EmailLog()
        smtp = email.login()
        
        email_subject = 'Error in executing CDAS Login Algorithm'
        lines = ['Error in log file is found today.']
        email_text = 'testing'
		
        email_img = 'test_pic.png'
        email_attachment = 'logs/error_log.log'
            
        msg = email.message(subject = email_subject, text = email_text, img =  email_img, attachment = email_attachment)
        email.sent(msg,smtp)
    
    def test_get_attachment_fail(self):
        email = EmailLog()
        smtp = email.login()
        
        email_subject = 'Error in executing CDAS Login Algorithm'
        lines = ['Error in log file is found today.']
        
        email_text = 'testing'
        email_img = 'test_pic'  # intentionally trigger error in path 
        email_attachment = 'logs/error_log.log'

        
        expectederror = "ERROR:libEmailLog.email_log:Error in Getting the Subject/Text/Image/Attachment."
        
        with self.assertLogs('libEmailLog.email_log', level='ERROR') as lc:
            msg = email.message(subject = email_subject, text = email_text, img =  email_img, attachment = email_attachment)
        self.assertTrue(expectederror in lc.output[0])
    
    def test_run(self):
        run()
    
if __name__ == '__main__':
    unittest.main(warnings='ignore')