import pandas as pd
import numpy as np
import datetime
import time
import sys
import traceback
import json
import os
#os.chdir("D:/2021/PythonCodes/Olam Automation")
import logging
import logging.config
logging.config.fileConfig('logging.ini',disable_existing_loggers=False)
#logging.config.fileConfig('../Olam Automation/logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)

from libOlamAutomation.olam_pallet_data import InputDatabaseExtraction, PalletCycleLogicChecking, TableProcess, OutputDatabase
from libOlamAutomation.olam_pallet_out import InputDatabaseExtraction_PalletOut, TableProcess_PalletOut, LogicChecking_PalletOut, OutputDatabase_PalletOut


def main():
    
    try: 
        
        #the second half to process data
        #os.chdir("D:/2021/PythonCodes/Olam Automation")
        dt_use=(datetime.date.today()-datetime.timedelta(1)).strftime('%Y-%m-%d')
        
        credential='config/servercred.json'

        #input extract from db
        input_pallet_data = InputDatabaseExtraction()
        
        df_end = input_pallet_data.extract_df(credential)
        
        df_patch = input_pallet_data.extract_patch(credential)
        
        #logic cyle checking
        logic_cycle_pallet_data  = PalletCycleLogicChecking()
        
        df_check=logic_cycle_pallet_data.cycle_check(df_end)
        
        process_pallet_data = TableProcess()
        
        if len(df_check)>0:
            
            df_trans=process_pallet_data.cycle_transform(df_check,dt_use)
            
            #df_trans=df_trans[~df_trans.isna().any(axis=1)]
            
            df_final=process_pallet_data.patch_silo(df_end,df_patch,process_pallet_data.patch_empty_pallet(df_end,df_trans))
            df_final=df_final.replace({np.nan:None})

            output_pallet_data = OutputDatabase()
            
            output_pallet_data.update_pallet_event(df_check,credential)

            output_pallet_data.update_pallet_silo(df_final,credential)  

            output_pallet_data.output_result(df_final,credential)  
            
            #run this once a week
            if datetime.date.today().weekday()==3:
                output_pallet_data.update_pallet_event_extra(credential)

        
        #must have weight out date
        input_pallet_out =  InputDatabaseExtraction_PalletOut()
        process_pallet_out = TableProcess_PalletOut()
        logic_check_pallet_out = LogicChecking_PalletOut()
        output_pallet_out =  OutputDatabase_PalletOut()
        
        df_out=process_pallet_out.process_df2(input_pallet_out.extract_df2(credential))

        if len(df_out)>0:
            
            df2_trans=process_pallet_data.cycle_transform(df_out,dt_use)
            
            #checkpoint
            errorls=logic_check_pallet_out.check_out_cycle(df2_trans)
            #checkdf=df2_trans.loc[df2_trans["EmptyPalletReturned_10"].notnull()]
            if len(errorls)>0:
                logger.info("There are some out-in-cycle pallets with error: total "+str(len(errorls)))
                    
            df2_final=df2_trans.loc[(~df2_trans["label"].isin(errorls))]
            df2_final=df2_final.replace({np.nan: None})
            
            output_pallet_out.output_result2(df2_final,credential)

        
    except Exception:

        ## Purpose: to keep track of all the Error

        logger.error(traceback.format_exc())

        sys.exit(1)

    finally:

        logging.shutdown()