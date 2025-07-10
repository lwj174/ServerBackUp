import pandas as pd
import logging

logger = logging.getLogger(__name__)

from libEmissionCalc.access import DatabaseAccess

class OutputDatabase:
    
    def output_db(self,a1,cred):
    
        extract_data = DatabaseAccess()
        
        conn=extract_data.db_conn(cred)
        cur=conn.cursor()

        a1=a1[["uid","Fuel_Consumption_All","tripMilageValue","AvgConsumed","dt","Fuel","remark"]]
        a1=a1.loc[a1["tripMilageValue"]>=0]
        try:
        
            sql1="INSERT INTO `emission_calculation` (`uid`,`Fuel_Consumption_All`,`tripMilageValue`,\
			`AvgConsumed`, `dt`, `Fuel`,`remark`) VALUES (" + "%s,"*(len(a1.columns)-1) + "%s)"
            if len(a1)>0:
                logger.info("Number of records: "+str(len(a1)))
                val1=list(a1.itertuples(index=False, name=None))
                cur.executemany(sql1, val1)
                conn.commit()

            conn.close()

        except Exception as e:
            
            logger.error(f'Fail to insert data into emission_calculation table. Error Message: {e.__class__}{e}')