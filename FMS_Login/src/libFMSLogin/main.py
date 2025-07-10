
import sys
import traceback
import pandas as pd
import numpy as np
import datetime
import os
import glob
import shutil
#os.chdir("D:/2021/PythonCodes/FMS_Login")
from libFMSLogin.output_db import Database #output_db_acc_login, output_db_acc_tree
from libFMSLogin.data_process import Process
from libFMSLogin.import_data import Email #download_rpt, email_login
#from import_data import download_rpt, email_login


import logging
import logging.config
logging.config.fileConfig('logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def main():

    try:
        
        download_folder=(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')

        cred="config/email_login.json"

        fms_login_email = Email()
        
        file_name=fms_login_email.get_login_data(download_folder,cred)
        
        fms_login_process = Process()
        
        acctree, final=fms_login_process.data_process(file_name[0])

        fms_login_db = Database()
        
        fms_login_db.output_db_acc_login(final,cred)

        fms_login_db.output_db_acc_tree(acctree,cred)

        
        files = glob.glob(download_folder+'*')
        for f in files:
            if os.path.exists(download_folder):
                shutil.rmtree(download_folder)
            
        
    except Exception:

        ## Purpose: to keep track of all the Error

        logger.error(traceback.format_exc())

        sys.exit(1)

    finally:

        logging.shutdown()