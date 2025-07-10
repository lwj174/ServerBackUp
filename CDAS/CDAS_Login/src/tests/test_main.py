#!/usr/bin/env python
# coding: utf-8

# In[1]:

import unittest
from unittest.mock import patch
import datetime

import os
import sys
sys.path.insert(0,'../../src')


from libCDAS.main import main

# In[2]:


class MainTestCase(unittest.TestCase):
    
    def setUp(self):
        
        pass
    
    def tearDown(self):
        
        folder =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
        
        if os.path.exists(folder):
            os.rmdir(folder)
        
    def test_CDAS(self):
        
        expectedlog_1 = 'Connected to IMAP Server with user'
        expectedlog_2 = 'Fetch list of messages from inbox'
        expectedlog_3 = "Downloaded and parsed mail 'User Login Report (CDAS)' with 1 attachments"
        
        with self.assertLogs('imbox', level='INFO') as lc:
            credential="config/email_login.json"
            fd =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
            main(fd,credential)
        self.assertTrue(expectedlog_1 in lc.output[0])
        self.assertTrue(expectedlog_2 in lc.output[1])
        self.assertTrue(expectedlog_3 in lc.output[2])
          
    def test_CDAS_error(self):
        
        expectederror = "FileNotFoundError: [Errno 2] No such file or directory: 'config/email_login.jso'"
        
        with self.assertRaises(SystemExit) as cm, self.assertLogs('libCDAS.main', level='ERROR') as lc :
            
            # error in json file
            credential="config/email_login.jso"
            fd =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
            main(fd,credential)

        self.assertTrue(expectederror in lc.output[0])
        
# In[3]:

if __name__ == '__main__':
    unittest.main()

