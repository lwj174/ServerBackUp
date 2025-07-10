
import requests
import datetime
import time
import sys
import traceback
import os
#os.chdir("D:/2021/PythonCodes/Olam Automation")
import logging
import logging.config

logging.config.fileConfig('logging.ini',disable_existing_loggers=False)
#logging.config.fileConfig('../Olam Automation/logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)

from libOlamDataFetch.input_api import ApiDataExtraction
from libOlamDataFetch.output_db import OutputDatabase

def main():
    
    try: 
        #the first half to extract & store data (daily)
        credential='config/servercred.json'
        
        dt_use=(datetime.date.today()-datetime.timedelta(1)).strftime('%Y-%m-%d')

        # get api token, url
        api_access = ApiDataExtraction(credential,dt_use)

        # extract dataframe from  api
        df_event = api_access.pallet_event_extract()
        
        df_patch = api_access.pallet_patch_silo_extract()
        
        # get db connection using credential
        db_access =  OutputDatabase(credential)
        
        # save dataframe output to database 
        db_access.pallet_event_output(df_event)

        db_access.pallet_silo_patch_output(df_patch)
                
    except Exception:

        ## Purpose: to keep track of all the Error

        logger.error(traceback.format_exc())

        sys.exit(1)

    finally:

        logging.shutdown()