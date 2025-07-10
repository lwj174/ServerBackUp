import zipfile
import os
import sys
import pandas as pd
import numpy as np
import datetime
import glob
import shutil
import logging

logger = logging.getLogger(__name__)

class Process:
    
    def __init__(self,download_folder):
        
        self.download_folder = download_folder
    
    def read_zipfile(self,fn1):
        
        try:
            with zipfile.ZipFile(fn1[0],"r") as zip_ref:
                zip_ref.extractall()
        except Exception as e:
            logger.error(f"Error in extracting zipfile. Error Message: {e.__class__}{e}")
            sys.exit(1)
            
    def process_dataframe(self):
        
        path = os.getcwd()
        
        try:
            csv_files = glob.glob(os.path.join(path, "*.xlsx"))
            
            df=pd.read_excel(csv_files[0],sheet_name='Logins')
            
        except Exception as e:
            logger.error(f"Error in reading excel file. Error Message: {e.__class__}{e}")
            

        dt_add=(datetime.date.today()-datetime.timedelta(1)).strftime('%Y-%m-%d')

        try: 
            a1=df.groupby('Grouping').apply(lambda group: group.iloc[1:])
            a1=a1.reset_index(drop=True)
            a1["Login time"]=dt_add+" "+a1["Login time"]
            a1["Logout time"]=dt_add+" "+a1["Logout time"]
            a1=a1.assign(Duration_Sec=pd.to_timedelta(a1["Duration"]).dt.total_seconds())
            a2=a1[["Grouping", "Login time", "Logout time", "Duration", "Duration_Sec","Count"]]
            
            return a2
        
        except Exception as e:
            logger.error(f"Error in processing the dataframe. Error Message: {e.__class__}{e}")
        
        
    def remove_file(self):
        
        try:
            shutil.rmtree(self.download_folder)
        except OSError as e:
            logger.error("Error: %s - %s." % (e.filename, e.strerror))

