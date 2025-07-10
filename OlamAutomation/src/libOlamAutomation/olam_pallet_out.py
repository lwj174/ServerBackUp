import pandas as pd
import numpy as np
import datetime
import time
import sys
import json
import os
#os.chdir("D:/2021/PythonCodes/Olam Automation")
import logging
#import logging.config
#logging.config.fileConfig('../Olam Automation/logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)

from libUtil.access import DatabaseAccess


class InputDatabaseExtraction_PalletOut:
    
    #separate table for pallets out in cycle
    def extract_df2(self,cred):
        
        try:
            
            conn = DatabaseAccess().db_conn(cred)
            cur=conn.cursor()
    
            q1='select * from palletEvent pe3 where label in (SELECT pe2.label \
                from palletEvent pe2 inner join (SELECT label ,max(createDt) as maxdt \
                                                from palletEvent pe where Used =0 group by label) t1 \
                on pe2.label=t1.label and pe2.createDt =t1.maxdt and \
                pe2.stateCode !=20 and pe2.stateCode !=10) and Used =0 and stateCode !=20' 
            
            #q1='select * from palletEvent where Used=1'
            df=pd.read_sql_query(q1,conn)
            conn.close()
            return df

        except Exception as e:
                
            logger.error(f'Fail to extract data from palletEvent table in the DataAnlytics database. Error Message: {e.__class__}{e}')
        
        
class TableProcess_PalletOut:
    
    def process_df2(self,df2):
        
        a1=df2.sort_values(["label","createDt"],ascending=False)
    
        #get the latest 25
        b1=a1.groupby("label").apply(lambda g: (g.stateCode.values == 25).argmax()+1).rename("cycle_length").reset_index()
        b2=a1.groupby("label").apply(lambda g: (g['stateCode']==25).sum()).rename("cycle_check").reset_index()

        b3=b1.loc[b1["label"].isin(b2["label"].loc[b2["cycle_check"]>0])]
        
        #logger.info("Total pallets out in cycle: "+str(len(b3)))
        
        a2=pd.merge(a1,b3,on='label')
        
        a3=a2.groupby("label",group_keys=False).apply(lambda x: x.head(x["cycle_length"].iloc[0]))
            
        a4=a3.drop_duplicates(["label","stateCode"],keep="last")
            
        return a4


class LogicChecking_PalletOut:
    
    #new function
    def cycle_null_check(self,df,state1,state2):
        t1=df.loc[df[state1].isnull()]
        e1=t1.loc[:,state1:state2]
        e1=e1.loc[~e1.isna().all(axis=1)]
        return e1.index
        


    #new function
    def check_out_cycle(self,df):
        
        errorls=[]    
        e1=df.loc[df["Weight_Out_25"].isnull()]
        errorls.extend(e1["label"].to_list())
        temp1=df.loc[df["Weight_Out_25"].notnull()]
        
        e2=self.cycle_null_check(temp1,"Loading_30","EmptyPalletPickup_60") 
        errorls.extend(df["label"].loc[df.index.isin(e2)].to_list())
        
        e3=self.cycle_null_check(temp1,"Client_Receipt_40","EmptyPalletPickup_60") 
        errorls.extend(df["label"].loc[df.index.isin(e3)].to_list())
        
        e4=self.cycle_null_check(temp1,"Silo_Production_50","EmptyPalletPickup_60") 
        errorls.extend(df["label"].loc[df.index.isin(e4)].to_list())
        
        if "EmptyPalletReturned_10" in df.columns:
            e5=df.loc[df["EmptyPalletReturned_10"].notnull()]
            errorls.extend(e5["label"].to_list())
        
        return errorls


class OutputDatabase_PalletOut:
    
    def output_result2(self,df2,cred):
        
        try:
            
            conn = DatabaseAccess().db_conn(cred)
            cur=conn.cursor()
    
            df2=df2.loc[:, :'EmptyPalletPickup_60']
            logger.info("Total pallets out in cycle: "+str(len(df2)))
            
            sql1="TRUNCATE TABLE OlamPalletOut"
            cur.execute(sql1)
            sql2="INSERT INTO `OlamPalletOut` (`PalletID`, `process_dt`, `Weight_Out_25`, \
                `Loading_30`,`Client_Receipt_40`,`Silo_Production_50`, `EmptyPalletPickup_60`) \
                VALUES (" + "%s,"*(len(df2.columns)-1) + "%s)"
            val2=list(df2.itertuples(index=False, name=None))
            cur.executemany(sql2, val2)
            conn.commit()
            
            conn.close()
            
        except Exception as e:
                
            logger.error(f'Fail to insert new data into OlamPalletOut table in the DataAnlytics database. Error Message: {e.__class__}{e}')