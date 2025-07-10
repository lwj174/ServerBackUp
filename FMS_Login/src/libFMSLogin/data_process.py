
import pandas as pd
import sys
import logging
logger = logging.getLogger(__name__)


class Process:
    
    def data_process(self,file_name):
        try:

            df_acc=pd.read_excel(file_name,sheet_name='Account tree')
            df=pd.read_excel(file_name,sheet_name='Logins')
            
            #modify account tree
            df_acc.columns=["Num","Type","Username"]

            df_acc=df_acc.assign(idnum=df_acc["Num"].str.split('.').str[0])

            #account layer 1
            t1=df_acc.groupby('idnum').head(1)

            t2=t1.loc[t1["Type"]=='Account']

            t2=t2.rename(columns={"Username":"Account1"})

            #account layer 2
            a1=df_acc.loc[~df_acc.index.isin(t2.index)]

            a1=a1.assign(idnum2=df_acc["Num"].str.split('.').str[0]+df_acc["Num"].str.split('.').str[1])

            b1=a1.groupby('idnum2').head(1)

            b2=b1.loc[b1["Type"]=='Account']

            b2=b2.rename(columns={"Username":"Account2"})

            a2=a1.loc[~a1.index.isin(b2.index)]

            a3=a2.loc[a2["Type"]=='User']

            r1=pd.merge(a3,t2[["idnum","Account1"]],on='idnum',how='left')

            r2=pd.merge(r1,b2[["idnum2","Account2"]],on='idnum2',how='left')

            r2["Account1"]=r2["Account1"].fillna('')
            r2["Account2"]=r2["Account2"].fillna('')

            r2=r2.assign(Accountname=r2["Account1"]+' / '+r2["Account2"])
            
            #newly added:
            r3=r2[["Account1","Account2","Accountname","Username"]]
                
            #modify logins
            dt_add=df["Grouping"].iloc[0]
            a1=df.iloc[1:]
            a0=a1.groupby("Grouping").head(1)
            a2=a1.loc[~a1.index.isin(a0.index)]
            a2=a2.assign(dt_today=dt_add)
            a2["Login time"]=dt_add+" "+a2["Login time"]
            a2["Logout time"]=dt_add+" "+a2["Logout time"]
            a2=a2.assign(Duration_Sec=pd.to_timedelta(a2["Duration"]).dt.total_seconds())


            a3=pd.merge(a2,r2,left_on="Grouping",right_on="Username",how='left')
            
            #add
            a3=a3.loc[a3["Duration_Sec"]>1]
            a4=a3[['Username','dt_today','Login time', 'Logout time','Duration_Sec','Count','Accountname']]
            
        except Exception as e:
            logger.error(e)
            
            sys.exit(1)
        
        return r3, a4