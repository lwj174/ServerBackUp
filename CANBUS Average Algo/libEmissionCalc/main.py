
import pandas as pd
import numpy as np
import traceback
import sys
#import os
#os.chdir("D:/2021/PythonCodes/Idling Emission Factor")
import datetime

import logging
import logging.config
logging.config.fileConfig('logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)

from libEmissionCalc.extraction import DatabaseExtraction
from libEmissionCalc.process import Process
from libEmissionCalc.output import OutputDatabase


def main():


    try:

        dist_factor=pd.DataFrame({"Vehicle":["Electric","Passenger","LGV","HGV","VHGV"],
                          "Distance_Emission_Factor":[0.09,0.143,0.262,0.391,1.316]})
        fuel_factor=pd.DataFrame({"Fuel":["Petrol","Diesel","Electric"],
                                "Fuel_Emission_Factor":[2.28644,2.67799,0.4168]})

        dt_latest=(datetime.date.today()-datetime.timedelta(0)).strftime('%Y-%m-%d')
        cred="config/db_cred.json"
        
        input = DatabaseExtraction()
        df_trip,df_eh=input.extract_data(cred,dt_latest)


        data_process = Process()

        a2, df_trip = data_process.fuel_emission_calc(df_trip,fuel_factor)
        
        b2, df_trip = data_process.dist_emission_calc(df_trip,dist_factor)
        
        c1 = data_process.idling_time_calc(df_eh)
        
        res2 = data_process.descriptive_stat(a2,b2,c1,df_trip)

        f1=data_process.check_90day(cred,res2)
        
        output = OutputDatabase()
        output.output_db(f1,cred)

    
    except Exception:

        ## Purpose: to keep track of all the Error

        logger.error(traceback.format_exc())

        sys.exit(1)

    finally:

        logging.shutdown()