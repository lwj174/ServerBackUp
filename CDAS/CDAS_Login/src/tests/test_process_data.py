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

from libCDAS.process_data import Process

class DatabaseTestCase(unittest.TestCase):
        
    def setUp(self):
        
        pass
    
    def tearDown(self):
        
        folder =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
        
        if os.path.exists(folder):
            os.rmdir(folder)
    
    def test_read_zipfile_error(self):
        download_folder =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
        test_process = Process(download_folder)
        
        expectederror = 'Error in extracting zipfile.'
            
        with self.assertRaises(SystemExit) as cm, self.assertLogs('libCDAS.process_data', level='ERROR') as lc :
            # create wrong path of zipfile
            test_process.read_zipfile(['wrong_path'])

        self.assertTrue(expectederror in lc.output[0])
            
    def test_process_dataframe_error(self):
        download_folder =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
        test_process = Process(download_folder)
        
        expectederror = 'Error in reading excel file.'
            
        with self.assertLogs('libCDAS.process_data', level='ERROR') as lc:
            # create wrong path of zipfile (Not change directory to 'CDAS LOGISTICS/xlsx' path)
            a2 = test_process.process_dataframe()
    
        self.assertTrue(expectederror in lc.output[0])
    
    def test_remove_file_error(self):
        download_folder =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
        test_process = Process('wrong_path')
        
        expectederror = 'Error: wrong_path - The system cannot find the path specified.'
            
        with self.assertLogs('libCDAS.process_data', level='ERROR') as lc:
            # create wrong path
            a2 = test_process.remove_file()
            
        self.assertTrue(expectederror in lc.output[0])
    
if __name__ == '__main__':
    unittest.main()