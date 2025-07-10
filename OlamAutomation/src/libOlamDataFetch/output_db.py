import sys
import logging
#import logging.config

logger = logging.getLogger(__name__)

from libUtil.access import DatabaseAccess

class OutputDatabase:

    def __init__(self,json_file):
        
        self.json_file = json_file
    
    def pallet_event_output(self,df_event):
        
        try :
            conn = DatabaseAccess().db_conn(self.json_file)

            cur=conn.cursor()
        
            if len(df_event)>0:
                df1=df_event[["createDt","createBy","pid","label","stateCode","pstat","eventCode","remark"]]
                sql1="INSERT INTO `palletEvent` (`createDt`, `createUser`, `pid`, `label`, `stateCode`,`pstat`, `eventCode`, `remark`) VALUES (" + "%s,"*(len(df1.columns)-1) + "%s)"
                val1=list(df1.itertuples(index=False, name=None))
                cur.executemany(sql1, val1)
                conn.commit()
                logger.info("Data saved for palletEvent.")
            else:
                logger.info("No data for pallet event.")

            conn.close()
    
        except Exception as e:
            
            logger.error(f'Fail to insert new data into palletEvent table in the DataAnlytics database. Error Message: {e.__class__}{e}')
        
        
        
    def pallet_silo_patch_output(self,df_patch):
        
        try :
            conn = DatabaseAccess().db_conn(self.json_file)

            cur=conn.cursor()
            
            if len(df_patch)>0:
                df2=df_patch[["createDt","createBy","evType","evTs","palletId","blNo","lotNo"]]
                sql2="INSERT INTO `palletSiloPatch` (`createDt`, `createUser`, `evType`, `evTs`,`palletId`, `blNo`, `lotNo`) VALUES (" + "%s,"*(len(df2.columns)-1) + "%s)"
                val2=list(df2.itertuples(index=False, name=None))
                cur.executemany(sql2, val2)
                conn.commit()
                logger.info("Data saved for palletSiloPatch.")
            else:
                logger.info("No data for patch silo.")

            conn.close()
            
        except Exception as e:
            
            logger.error(f'Fail to insert new data into palletSiloPatch table in the DataAnlytics database. Error Message: {e.__class__}{e}')
            
        