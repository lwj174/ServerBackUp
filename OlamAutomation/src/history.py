# -*- coding: utf-8 -*-
"""
Created on Fri Jul 21 16:14:05 2023

@author: Wen Jing
"""

#for olam historical data
import pandas as pd
import os
os.chdir("D:/2021/PythonCodes/Olam Automation")
import datetime
from olam_data_fetch import user_login, data_extract, save_db

from olam_automation import *

#clean up old records
#1. extract from Zoho the dirty records (not complete cycle/end before start)
a1=pd.read_csv("Olam_Pallet_Raw_Data (1).csv")
a2=pd.read_csv("Olam_Pallet_Raw_Data.csv")
df=pd.concat([a1,a2])


#2.extract data from 01 Apr & search day by day if these labels are within
credential='servercred.json'
access_token=user_login(credential)

df_old_event=pd.DataFrame()
df_old_patch=pd.DataFrame()

#start from 1 Apr to 31 May - 117 to 57
for i in range(61):
    dt_use=(datetime.date.today()-datetime.timedelta(117-i)).strftime('%Y-%m-%d')
    df_event, df_patch=data_extract(credential,access_token,dt_use)
    if len(df_event)>0:
        df1=df_event.loc[df_event["label"].isin(df["Pallet Id"].to_list())]
        if len(df1)>0:
            df_old_event=df_old_event.append(df1)
    if len(df_patch)>0:
        df2=df_patch.loc[df_patch["palletId"].isin(df["Pallet Id"].to_list())]
        if len(df2)>0:
            df_old_patch=df_old_patch.append(df2)


#3.store the processed data in

#completed cycle
df_old_event=df_old_event.reset_index(drop=True)
df_old_patch=df_old_patch.reset_index(drop=True)
df_check=cycle_check(df_old_event)
df_trans=cycle_transform(df_check,dt_use)

df_final=patch_silo(df_old_event,df_old_patch,patch_empty_pallet(df_old_event,df_trans))
output_result(df_final,credential)



#complete cycles 2 & 3
df_left=df_old_event.loc[~df_old_event.index.isin(df_check.index)]

df_final=df_final.assign(start_date=pd.to_datetime(df_final["Weight_Out_25"],infer_datetime_format=True))
df=df.assign(start_date=pd.to_datetime(df["Weight Out Date"],infer_datetime_format=True))

a3=pd.merge(df,df_final[["label","start_date"]],left_on=["Pallet Id","start_date"],
            right_on=["label","start_date"],how="left")

a4=a3.loc[a3["label"].isnull()].drop(columns=['label'])

df_check=cycle_check(df_left)
df_trans=cycle_transform(df_check,dt_use)

df_final=patch_silo(df_left,df_old_patch,patch_empty_pallet(df_left,df_trans))
output_result(df_final,credential)


df_left2=df_left.loc[~df_left.index.isin(df_check.index)]

df_final=df_final.assign(start_date=pd.to_datetime(df_final["Weight_Out_25"],infer_datetime_format=True))

a4=pd.merge(a4,df_final[["label","start_date"]],left_on=["Pallet Id","start_date"],
            right_on=["label","start_date"],how="left")

a5=a4.loc[a4["label"].isnull()].drop(columns=["label"])

df_check=cycle_check(df_left2)
df_trans=cycle_transform(df_check,dt_use)

df_final=patch_silo(df_left2,df_old_patch,patch_empty_pallet(df_left2,df_trans))
output_result(df_final,credential)

df_final=df_final.assign(start_date=pd.to_datetime(df_final["Weight_Out_25"],infer_datetime_format=True))
a5=pd.merge(a5,df_final[["label","start_date"]],left_on=["Pallet Id","start_date"],
            right_on=["label","start_date"],how="left")

a6=a5.loc[a5["label"].isnull()].drop(columns=["label"])


#incomplete cycles
df_inc=df_left2.loc[~df_left2.index.isin(df_check.index)]


save_db(df_inc,df_old_patch,credential)













def extract_df(cred,palletID):
    conn=db_conn(db_login(cred))
    cur=conn.cursor()
    
    q1='select pe.* from palletEvent pe where pe.Used=0 and pe.stateCode!=20 and pe.label="'+\
        str(palletID)+'"'
    
    #q1='select * from palletEvent where Used=1'
    df=pd.read_sql_query(q1,conn)
    conn.close()
    return df




def cycle_transform(df,dt_use):
    #row to column
    palletcycle=pd.pivot_table(df,values='createDt', index='label',columns='stateCode')

    palletcycle.columns = palletcycle.columns.map(str)

    palletcycle=palletcycle.assign(process_dt=dt_use)
    try:
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


    except KeyError:
        pass
    
    #drop all columns before process_dt
    palletcycle=palletcycle.loc[:, 'process_dt':]
    palletcycle=palletcycle.reset_index()
    
    return palletcycle



df1=pd.read_excel("Olam_Pallet_Raw_Data_WrongDates.xlsx")

df2=pd.DataFrame()

for x in df1["Pallet Id"].to_list():
    temp=extract_df(credential,x)
    if len(temp)>0:
        if len(temp.loc[temp["stateCode"].isin([50,60,10])])>0:
            df2=df2.append(temp,ignore_index=True)

df2=df2.sort_values(["label","createDt"])
#df3=df2.loc[~df2.index.isin([27,28,29,30,34,38,39,40])]

df4=cycle_transform(df2,dt_use)

df5=patch_empty_pallet(df2,df4)
df6=patch_silo(df2,df_patch,df5)

df6.to_excel("Historical_Data2.xlsx",index=False)


#mark as used in those ids








