import pandas as pd
import logging

logger = logging.getLogger(__name__)

from libCANBUS.access import DatabaseAccess

class DatabaseExtraction:
    
    #get universal list for device type, unit name
    #extract fuel or battery details by UID
    def extract_all(self, cred):
        
        extract_data = DatabaseAccess()
        
        conn=extract_data.db_conn(cred)
        cur=conn.cursor()
        
        try:
            
            q1="select uu.*, fc.clientName as companyName from unit_update uu \
                left join fms2_client fc on uu.companyid =fc.clientId where uu.Remove_Flag !=1"
            df=pd.read_sql_query(q1,conn)
            conn.close()
            return df
        
        except Exception as e:
            
            logger.error(f'Fail to read unit_update table from fms database. Error Message: {e.__class__}{e}') 


    def extract_dtl(self,cred,dt_use):
        
        extract_data = DatabaseAccess()
        
        conn=extract_data.db_conn(cred)
        cur=conn.cursor()
        
        try:
            q2="select uid, max(CAST(fuelConsumed AS UNSIGNED)) as max_fuelconsume, \
                sum(CAST(fuelConsumed AS UNSIGNED)) as sum_fuelconsume, max(fuelLvlInit) as max_fuellv,\
                max(fuelConsumedInit) as max_fuelrd, max(batteryLvlInit) as max_battery, min(batteryLvlInit) as min_battery \
                from fms2_trip ft where dt like '"+ dt_use +"%' group by uid "
            df2=pd.read_sql_query(q2,conn)
            conn.close()
            return df2
        
        except Exception as e:
            
            logger.error(f'Fail to read fms2_trip table from fms database. Error Message: {e.__class__}{e}') 

    
    def get_canbus_device(self,dtl_df,univ_df):
        t1=dtl_df.loc[(dtl_df["max_fuelconsume"]>0)|(dtl_df["max_battery"]>0)|(dtl_df["max_fuellv"]>0)|(dtl_df["max_fuelrd"]>0)]
        t1=t1.assign(category="")
        t1.loc[(t1["max_fuellv"]>0)|(t1["max_fuelconsume"]),"category"]="Fuel"
        t1.loc[t1["max_battery"]>0,"category"]="Battery"
        t2=pd.merge(t1,univ_df[["uid","car_plate","Device_Type","createts","companyName"]],on="uid")
        #add those manual units
        t0=univ_df.loc[univ_df["canbus_device"]=='Yes']
        t0=t0.rename(columns={"canbus_category":"category"})
        t0=t0[["uid","car_plate","Device_Type","createts","companyName","category"]]
        t2=pd.concat([t2,t0[~t0["uid"].isin(t2["uid"])]])
        return t2
    
    
    #get alerts
    #first to extract trip info for the list of devices
    #separate fuel and battery
    def get_gps_dist(self,cred,df,dt_latest):
        extract_data = DatabaseAccess()
        
        conn=extract_data.db_conn(cred)
        cur=conn.cursor()
        uid_ls=df["uid"].to_list()
        uid_sql=','.join(str(x) for x in uid_ls)
        q1="select uid,distance_new from algo_output ft where dt ='"+ dt_latest +"' and uid in ("+uid_sql+")"
        df1=pd.read_sql_query(q1,conn)
        conn.close()
        return df1
	
	
	
    def get_trip(self,cred,df,dt_latest):
        
        extract_data = DatabaseAccess()
        
        conn=extract_data.db_conn(cred)
        cur=conn.cursor()
        
        try:
            
            uid_ls=df["uid"].to_list()
            uid_sql=','.join(str(x) for x in uid_ls)
            q3="select * from fms2_trip ft where dt ='"+ dt_latest +"' and uid in ("+uid_sql+")"
            df3=pd.read_sql_query(q3,conn)
            conn.close()
            return df3
        
        except Exception as e:
            
            logger.error(f'Fail to read fms2_trip table from fms database. Error Message: {e.__class__}{e}') 