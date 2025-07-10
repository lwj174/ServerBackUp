import pandas as pd
import logging
import datetime

logger = logging.getLogger(__name__)

from libEmissionCalc.access import DatabaseAccess


class DatabaseExtraction:
    
    #extract trip & engine hour
    def extract_data(self,cred,dt_latest):
        
        extract_data = DatabaseAccess()
        
        conn=extract_data.db_conn(cred)
        cur=conn.cursor()
        
        try: 
            
            q1="select ft.uid, ft.dt, ft.unitName ,ft.clientId ,ft.beginDt ,ft.endDt ,\
            ft.tripMilageValue ,ft.fuelConsumed ,ft.fuelLvlInit ,ft.fuelLvlFinal ,\
            ft.fuelConsumedInit ,ft.fuelConsumedFinal ,ft.batteryLvlInit ,\
            ft.batteryLvlFinal ,ft.batteryEnergyConsInit ,ft.batteryEnergyConsFinal , ft.olometerInit, ft.olometerFinal,\
            fvp.primaryFuelType , fvp.batteryType , fvp.batteryCapacity , fvp.vehicleType\
            from fms2_trip ft inner join (select uid, primaryFuelType, batteryType, vehicleType, batteryCapacity \
            from fms2_vehicle_profile union select uid, null, null, null, null from unit_update where \
            canbus_device='Yes' and remove_flag=0) fvp on ft.uid=fvp.uid where ft.dt='"+dt_latest+"'"
            df1=pd.read_sql_query(q1,conn)
        
        except Exception as e:
            
            logger.error(f'Fail to read fms2_trip or fms2_vehicle_profile table from fms database. Error Message: {e.__class__}{e}') 
            
        try: 
            
            q2="select uid,dt,beginDt ,endDt ,engineHrsSec ,inMotionSec ,idlingSec \
            from fms2_engine_hours feh where dt='"+dt_latest+"' and uid in \
            (select distinct uid from fms2_vehicle_profile fvp)"
            df2=pd.read_sql_query(q2,conn)
            
        except Exception as e:
            
            logger.error(f'Fail to read fms2_engine_hours or fms2_vehicle_profile table from fms database. Error Message: {e.__class__}{e}') 
            
        conn.close()
        return df1, df2

    #new step: calculate 90 day average
    def extract_90day(self,cred):
        
        df_90=(datetime.date.today()-datetime.timedelta(90)).strftime('%Y-%m-%d')
        
        extract_90day = DatabaseAccess()
        
        conn=extract_90day.db_conn(cred)
        
        cur=conn.cursor()
        
        try: 
            
            q1="select * from emission_calculation ec2 where id>=\
                (select min(id) from emission_calculation ec where dt='"+df_90+"') and (remark is null or remark!='90day') and AvgConsumed>0"
            df1=pd.read_sql_query(q1,conn)
        
        except Exception as e:
            
            logger.error(f'Fail to read emission_calculation table from fms database. Error Message: {e.__class__}{e}') 
                
        conn.close()
            
        return df1