import pandas as pd
import logging

logger = logging.getLogger(__name__)

from libCANBUS.access import DatabaseAccess


class OutputDatabase:
    
    
    #output to DB
    def output_DB(self,a1,cred):
        
        extract_data = DatabaseAccess()
        
        conn=extract_data.db_conn(cred)
        cur=conn.cursor()

        try:
        
            sql1="INSERT INTO `canbus_alert` (`Trip_ID`, `dt`, `uid`,`unitName`, `beginDt`, \
        `endDt`, `initLoc`, `finalLoc`,`durationSecond`, `trip_mileage_km`, \
        `final_mileage_km`, `speedAvgValue`, `speedMaxValue`, `fuelConsumed`, \
        `fuelLvlInit`, `fuelLvlFinal`,`batteryLvlInit`,`batteryLvlFinal`, \
        fuelConsumedInit,fuelConsumedFinal,olometerInit, olometerFinal, can_mileage,\
        `remark`, `total_trip_records`,`total_issue_records`,`category`, `Device_Type`, \
        `companyName`) VALUES (" + "%s,"*(len(a1.columns)-1) + "%s)"
            if len(a1)>0:
                val1=list(a1.itertuples(index=False, name=None))
                cur.executemany(sql1, val1)
                conn.commit()

            conn.close()

        except Exception as e:
            
            logger.error(f'Fail to insert data into canbus_alert table. Error Message: {e.__class__}{e}')


    #output device list to DB add column in unit update
    def update_DB(self,dev_df,cred):
        
        extract_data = DatabaseAccess()
        
        conn=extract_data.db_conn(cred)
        cur=conn.cursor()
        
        #compare with previous list
        q1="select uid from unit_update where Remove_Flag !=1 and canbus_device='Yes'"
        df=pd.read_sql_query(q1,conn)
        
        new_df=dev_df.loc[~dev_df["uid"].isin(df["uid"])]
        logger.info("Number of New CAN BUS devices: "+str(len(new_df)))
        
        try:
        
            for i in range(len(new_df)):
                uid=new_df["uid"].iloc[i]
                fueltype=new_df["category"].iloc[i]
                sql2="UPDATE `unit_update` SET canbus_device='Yes', canbus_category='"+ fueltype +"' where uid="+str(uid)
                cur.execute(sql2)
                conn.commit()
            
            conn.close()
            
        except Exception as e:
            
            logger.error(f'Fail to update data in unit_update table. Error Message: {e.__class__}{e}')
