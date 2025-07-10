import traceback
import sys
import pandas as pd
import numpy as np
import datetime
import logging
import logging.config
logging.config.fileConfig('logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)


from libCANBUS.extraction import DatabaseExtraction
from libCANBUS.checking import DataChecking
from libCANBUS.output import OutputDatabase


def main():
    
    try: 
        
        dt_use=(datetime.date.today()-datetime.timedelta(1)).strftime('%Y-%m-%d')
        dt_latest=(datetime.date.today()-datetime.timedelta(1)).strftime('%Y-%m-%d')
        
        cred="config/db_cred.json"
        
        input = DatabaseExtraction()
        
        univ_df=input.extract_all(cred)
        dtl_df=input.extract_dtl(cred, dt_use)
        
        #about 27 devices? export to excel
        res_df=input.get_canbus_device(dtl_df,univ_df)
        
        fuel_df=res_df.loc[res_df["category"]=='Fuel']
        battery_df=res_df.loc[res_df["category"]=='Battery']
        #question: fuel level>100 okay?

        #could be case where fuel level final>fuel level init

        #question: or fuel consumption by time not mileage?

        data_checking = DataChecking()
        
        fuel_final=data_checking.fuel_check(cred,fuel_df,dt_latest)
        battery_final=data_checking.battery_check(cred,battery_df,dt_latest)

        result=pd.concat([fuel_final,battery_final]).reset_index(drop = True)
        if len(result)>0:
            result["beginDt"]=result["beginDt"].dt.strftime('%Y-%m-%d %H:%M:%S')
            result["endDt"]=result["endDt"].dt.strftime('%Y-%m-%d %H:%M:%S')
	
        result.insert(2, "uid", result.pop("uid"))
        result[["fuelConsumedInit","fuelConsumedFinal","olometerInit","olometerFinal"]]=\
        result[["fuelConsumedInit","fuelConsumedFinal","olometerInit","olometerFinal"]].fillna(0)
        
        output = OutputDatabase()
        
        output.output_DB(result,cred)

        output.update_DB(res_df,cred)
        
        #update logic on 9/19
        
    except Exception:

        ## Purpose: to keep track of all the Error

        logger.error(traceback.format_exc())

        sys.exit(1)

    finally:

        logging.shutdown()