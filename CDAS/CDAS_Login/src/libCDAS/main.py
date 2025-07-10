
from imbox import Imbox
import os
import traceback
import sys

# os.chdir("D:/2021/PythonCodes/CDAS_Login")

from libCDAS.output_db import Database
from libCDAS.input_email import Email
from libCDAS.process_data import Process

import logging
import logging.config
logging.config.fileConfig('logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)

def main(download_folder,cred):

    try:
        
        if not os.path.isdir(download_folder):
            os.makedirs(download_folder, exist_ok=True)
        
        # login email
        cdas_email = Email()
        username,password,host,sender=cdas_email.login_cred(cred) 
        mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)

        # download attachment 
        fn1,n1=cdas_email.download_rpt(download_folder,mail,sender)

        # extract excel file from zipfile (attachment)
        cdas_process = Process(download_folder)
        
        os.chdir(download_folder)
        cdas_process.read_zipfile(fn1)
        
        # read & process the excel dataframe
        os.chdir('CDAS LOGISTICS/xlsx')
        a2 = cdas_process.process_dataframe()
        
        # go back to [src] directory since previous is from  
        os.chdir('../../../')   # go back to [src] directory since previous currect directory is src\Nov\Nov 28\CDAS LOGISTICS\xlsx
        
        # export processed data to Mariadb database
        cdas_db = Database()
        cdas_db.output_db(a2,cred)
        
        # remove the zipfile(attachment)
        cdas_process.remove_file()
    
    except Exception:

        ## Purpose: to keep track of all the Error

        logger.error(traceback.format_exc())

        sys.exit(1)

    finally:

        logging.shutdown()