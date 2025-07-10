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


class InputDatabaseExtraction:
    
     
        
    def extract_df(self,cred):
        
        try: 
            conn = DatabaseAccess().db_conn(cred)
            cur=conn.cursor()
            
            q1='select pe.*, t1.cnt from palletEvent pe left join ( \
                select label, min(stateCode) as min_code, max(stateCode) as max_code, \
                count(stateCode) as cnt from palletEvent where `Used`=0 and stateCode !=20 \
                group by label ) t1 on pe.label =t1.label where t1.min_code=10 and t1.max_code=60 \
                and t1.cnt>=6 and pe.Used=0' 
            
            
            #q1='select * from palletEvent where Used=1'
            df=pd.read_sql_query(q1,conn)
            conn.close()
            return df

        except Exception as e:
                
            logger.error(f'Fail to extract data from palletEvent table in the DataAnlytics database. Error Message: {e.__class__}{e}')


    def extract_patch(self,cred):
        
        try: 
            conn = DatabaseAccess().db_conn(cred)
            cur=conn.cursor()
            
            q1='select * from palletSiloPatch where `Used`=0'    
            df=pd.read_sql_query(q1,conn)
            conn.close()
            return df

        except Exception as e:
                
            logger.error(f'Fail to extract data from palletSiloPatch table in the DataAnlytics database. Error Message: {e.__class__}{e}')
    
    

class PalletCycleLogicChecking:
    
    #check state order
    def check_cycle(self,statels,cyclels):
        if len(statels)<len(cyclels):
            return False
        else:
            for i in range(len(cyclels)):
                if cyclels[i] in statels:
                    idx=statels.index(cyclels[i])
                    statels=statels[idx:]
                else:
                    return False
            
            return True


    #still need the index to extract 1 complete cycle
    def extract_cycle(self,statels,cyclels):
        idx_list=[]
        for i in range(len(cyclels)):
            if cyclels[i] in statels:
                idx=statels.index(cyclels[i])
                idx_list.append(idx)
                statels=statels[idx:]
            else:
                logger.error("Error with extracting cycle!")
                idx_list=[]
                break
                
        return idx_list


    def cycle_check(self,df_end):
        
        #check for the cycle: must start with 25 & end with 10
        a1=df_end.sort_values(["label","createDt"],ascending=False)
        logger.info("Before checking, total number of rows: "+str(len(a1)))
        
        b1=a1.groupby("label").first().reset_index() #latest state
        b2=a1.groupby("label").last().reset_index() #earliest state

        #complete cycle
        b3=b1.loc[b1["stateCode"]==10]
        b4=b2.loc[b2["stateCode"]==25]

        c1=b3.loc[b3["label"].isin(b4["label"])]

        a2=a1.loc[a1["label"].isin(c1["label"])]
        #check point
        #cp=a2.groupby("label")["stateCode"].agg({"count","min","max","nunique"}).reset_index()
        #drop duplicates: if there are 2 complete cycles it would take 1 for this round
        #problem is it might be a combination of 2 incomplete cycle
        a3=a2.drop_duplicates(["label","stateCode"],keep="first")
        
        #not complete cycle: more than 1 cycle?
        a4=a1.loc[~a1["label"].isin(c1["label"])]    
        standardlist=[10,60,50,40,30,25]
    
        for label in list(a4["label"].unique()):
            temp=a4.loc[a4["label"]==label]
            statelist=temp["stateCode"].to_list()
            if self.check_cycle(statelist,standardlist):
                #if there's more than 1 cycle, only take the first one
                idxls=self.extract_cycle(statelist,standardlist)
                if len(idxls)>0:
                    t1=temp.iloc[idxls[0]:]
                    t2=t1.loc[t1["stateCode"]==25] #descending order guarantees nearest 25 is the first
                    cycle1=temp.loc[(temp["createDt"]<=t1.iloc[0]["createDt"])&(temp["createDt"]>=t2.iloc[0]["createDt"])]
                a3=a3.append(cycle1)
        
        #check point2
        #cp=a3.groupby("label")["stateCode"].agg({"count","min","max","nunique"}).reset_index()
        a3=a3.drop_duplicates(["label","stateCode"],keep="first")
        
        #check point3
        #cp=a3.groupby("label")["createDt"].agg({"min","max"}).reset_index()
        #cp0=a1.groupby("label")["stateCode"].count().reset_index()
        #cp=pd.merge(cp,cp0,on="label")
        #cp=cp.assign(diff=(cp["max"]-cp["min"])/(1000*3600*24))
        #takeout=cp.loc[(cp["diff"]>10)&(cp["max"]<1688140800000)]
        #a3=a3.loc[~a3["label"].isin(takeout["label"])]    
        logger.info("After checking, total number of rows: "+str(len(a3)))
        return a3
    
    
class TableProcess:
    
    def cycle_transform(self,df,dt_use):
        
        df=df.loc[df["stateCode"]!=20]    
        #row to column    
        palletcycle=pd.pivot_table(df,values='createDt', index='label',columns='stateCode', aggfunc='first')
            
        palletcycle.columns = palletcycle.columns.map(str)
        
        palletcycle=palletcycle.assign(process_dt=dt_use)
        
        usercycle=pd.pivot_table(df,values='createUser', index='label',columns='stateCode', aggfunc='first')
        usercycle.columns = usercycle.columns.map(str)
        try:
        
            palletcycle=palletcycle.assign(Weight_Out_25=pd.to_datetime(palletcycle['25'], unit='ms')\
                                .dt.tz_localize('UTC').dt.tz_convert('Singapore').dt.strftime('%Y-%m-%d'))

            palletcycle=palletcycle.assign(Loading_30=pd.to_datetime(palletcycle['30'], unit='ms')\
                                .dt.tz_localize('UTC').dt.tz_convert('Singapore').dt.strftime('%Y-%m-%d'))
        
            palletcycle=palletcycle.assign(Client_Receipt_40=pd.to_datetime(palletcycle['40'], unit='ms')\
                                .dt.tz_localize('UTC').dt.tz_convert('Singapore').dt.strftime('%Y-%m-%d'))

            palletcycle=palletcycle.assign(Silo_Production_50=pd.to_datetime(palletcycle['50'], unit='ms')\
                                .dt.tz_localize('UTC').dt.tz_convert('Singapore').dt.strftime('%Y-%m-%d'))

            palletcycle=palletcycle.assign(EmptyPalletPickup_60=pd.to_datetime(palletcycle['60'], unit='ms')\
                                .dt.tz_localize('UTC').dt.tz_convert('Singapore').dt.strftime('%Y-%m-%d'))

            palletcycle=palletcycle.assign(EmptyPalletReturned_10=pd.to_datetime(palletcycle['10'], unit='ms')\
                                .dt.tz_localize('UTC').dt.tz_convert('Singapore').dt.strftime('%Y-%m-%d'))
                    
            usercylce=usercycle[['10','25','30','40','50','60']]

        except KeyError:
            pass
        
        
        #drop all columns before process_dt
        palletcycle=palletcycle.loc[:, 'process_dt':]
        #join username
        fullcycle=pd.merge(palletcycle,usercycle,left_index=True,right_index=True,how='left')
        fullcycle=fullcycle.reset_index()
        
        return fullcycle



    def patch_empty_pallet(self,df,df_trans):
        #check patch actions
        temp=df.loc[df["remark"].str.contains('231026',na=False)]
        df_trans=df_trans.assign(Patch_Empty_Pallet='')
        df_trans.loc[df_trans["label"].isin(temp["label"]),"Patch_Empty_Pallet"]='Yes'

        return df_trans


    def patch_silo(self,df,df_patch,df_trans):
        #check patch actions
        temp=df.loc[df["label"].isin(df_patch["palletId"])]
        a1=temp.groupby("label")["createDt"].agg(["min","max"]).reset_index()     
        a2=pd.merge(a1,df_patch[["palletId","evTs"]],left_on="label",right_on="palletId")
        b1=a2.loc[(a2["evTs"]<a2["max"])&(a2["evTs"]>a2["min"])]
        df_trans=df_trans.assign(Patch_Silo='')
        df_trans.loc[df_trans["label"].isin(b1["label"]),"Patch_Silo"]='Yes'

        return df_trans
    
    
class OutputDatabase:
    
    def update_pallet_event(self,df,cred):
        
        try:
            
            #mark used rows 
            conn = DatabaseAccess().db_conn(cred)

            cur=conn.cursor()
            id_ls=df["id"].to_list()
            id_all=','.join(str(x) for x in id_ls)
            sql1="UPDATE `palletEvent` SET Used=1 where id in ("+id_all+")"
            cur.execute(sql1)
            conn.commit()
            
            conn.close()
        
        except Exception as e:
                
            logger.error(f'Fail to update palletEvent table in the DataAnlytics database. Error Message: {e.__class__}{e}')


    def update_pallet_silo(self,df,cred):
        
        try:
            
            #mark used rows 
            conn = DatabaseAccess().db_conn(cred)
            cur=conn.cursor()
            
            temp=df.loc[df["Patch_Silo"]=='Yes']
            if len(temp)>0:
                label_ls=temp["label"].to_list()
                label_all=','.join(f'"{x}"' for x in label_ls)
                sql1="UPDATE `palletSiloPatch` SET Used=1 where palletId in ("+label_all+")"
                cur.execute(sql1)
                conn.commit()
            
            conn.close()

        except Exception as e:
                
            logger.error(f'Fail to update palletSiloPatch table in the DataAnlytics database. Error Message: {e.__class__}{e}')



    def update_pallet_event_extra(self,cred):
        
        try:
            
            #mark unused but expired rows?
            conn = DatabaseAccess().db_conn(cred)

            cur=conn.cursor()
            sql0="SELECT pe2.*,tp1.maxdt from palletEvent pe2 inner join (select label , max(createDt) as maxdt \
                from palletEvent pe where Used =1 group by label ) tp1 \
                on pe2.label=tp1.label and pe2.createDt <tp1.maxdt where Used =0 and stateCode!=20"
            df=pd.read_sql_query(sql0,conn)
                
            id_ls=df["id"].to_list()
            id_all=','.join(str(x) for x in id_ls)
            sql1="UPDATE `palletEvent` SET Used=1 where id in ("+id_all+")"
            cur.execute(sql1)
            conn.commit()
            
            conn.close()

        except Exception as e:
                
            logger.error(f'Fail to update palletEvent table in the DataAnlytics database. Error Message: {e.__class__}{e}')



    def output_result(self,df,cred):
        
        try:
            
            conn = DatabaseAccess().db_conn(cred)
            cur=conn.cursor()
    
            df=df.assign(Patch_Diff=0)
            df.loc[(df["Patch_Empty_Pallet"]=='Yes')&(df["Patch_Silo"]==''),"Patch_Diff"]=1
            logger.info("New data added: "+str(len(df))+" pallet cycles with difference " \
                        +str(len(df.loc[df["Patch_Diff"]==1])))
            
            #print (df)
            
            sql1="INSERT INTO `OlamPalletData` (`PalletID`, `process_dt`, `Weight_Out_25`, \
                `Loading_30`,`Client_Receipt_40`,`Silo_Production_50`, `EmptyPalletPickup_60`, \
                `EmptyPalletReturned_10`,`EmptyPalletReturned_By`,`Weight_Out_By`,`Loading_By`,\
                `Client_Receipt_By`,`Silo_Production_By`,`EmptyPalletPickup_By`,\
                `Patch_Empty_Pallet`, `Patch_Silo`,`PatchDifference_EmptyPalletvsSilo`) \
                VALUES (" + "%s,"*(len(df.columns)-1) + "%s)"
            val1=list(df.itertuples(index=False, name=None))
            cur.executemany(sql1, val1)
            conn.commit()
            
            conn.close()
            
        except Exception as e:
                
            logger.error(f'Fail to insert new data to OlamPalletData table in the DataAnlytics database. Error Message: {e.__class__}{e}')