import unittest
from unittest.mock import patch
import datetime 
import os
import sys
sys.path.insert(0,'../../src')

import logging
import logging.config
logging.config.fileConfig('logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)

from libCDAS.output_db import Database

class DatabaseTestCase(unittest.TestCase):
    
    def setUp(self):
        
        pass
    
    def tearDown(self):
        
        folder =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
        
        if os.path.exists(folder):
            os.rmdir(folder)
    
    def test_db_conn_error(self):
        
        expectederror = 'Error connecting to MariaDB Platform:'
        
        with self.assertRaises(SystemExit) as cm, self.assertLogs('libCDAS.output_db', level='ERROR') as lc:
            test_db = Database()
            db_credential = test_db.login_cred('config/email_login.json')
            
            # create wrong credential (wrong username)
            # not able to make changes in tuple, need to change to list 
            db_credential = list(db_credential)
            db_credential[0] = 'error_user'
            db_credential = tuple(db_credential)
    
            conn = test_db.db_conn(db_credential)
        
        self.assertTrue(expectederror in lc.output[0])
            
    def test_output_db_error(self):
        
        expectederror = 'Fail to insert data into FMSLoginHist table.'
            
        with self.assertLogs('libCDAS.output_db', level='ERROR') as lc:
            
            # create wrong data to be inserted
            test_db = Database()
            test_a2 = ['0','1','2']
            test_db.output_db(test_a2,'config/email_login.json')
            
        self.assertTrue(expectederror in lc.output[0])
    
        
        
if __name__ == '__main__':
    unittest.main()