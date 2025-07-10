import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)
from libEmissionCalc.extraction import DatabaseExtraction


class Process:
    
    def fuel_emission_calc(self,df_trip,fuel_factor):

        #calculate fuel emission
        df_trip=df_trip.assign(Fuel='')
        df_trip.loc[df_trip["batteryType"]!='',"Fuel"]="Electric"
        df_trip.loc[df_trip["batteryCapacity"]!='',"Fuel"]="Electric"
        df_trip.loc[df_trip["primaryFuelType"]!='',"Fuel"]=df_trip["primaryFuelType"].str.title()

        a1=pd.merge(df_trip,fuel_factor,on="Fuel",how="left")
        
        a1=a1.fillna(0)

        a1=a1.assign(Fuel_Consumption_All=np.maximum.reduce([a1["fuelConsumed"],\
                    a1["fuelLvlInit"]-a1["fuelLvlFinal"],a1["fuelConsumedFinal"]-a1["fuelConsumedInit"]]))

        a1["batteryCapacity"]=pd.to_numeric(a1["batteryCapacity"].str.split(' ').str[0].str.replace(',','')) 
        a1.loc[a1["Fuel"].str.contains("Electric",na=False),"Fuel_Consumption_All"]=(a1["batteryLvlInit"]-a1["batteryLvlFinal"])*a1["batteryCapacity"]/100
        a1.loc[a1["Fuel_Consumption_All"]<0,"Fuel_Consumption_All"]=0
        #a1=a1.assign(Fuel_Emission=a1["Fuel_Consumption_All"]*a1["Fuel_Emission_Factor"])

        a2=a1.groupby("uid")[["Fuel_Consumption_All"]].sum()
        
        return a2, df_trip
    

    def dist_emission_calc(self,df_trip,dist_factor):
    
        df_trip.loc[df_trip["batteryType"]!='',"Vehicle"]="Electric"
        df_trip.loc[df_trip["batteryCapacity"]!='',"Vehicle"]="Electric"


        b1=pd.merge(df_trip,dist_factor,on="Vehicle",how="left")
        #b1=b1.assign(Dist_Emission=b1["tripMilageValue"]*b1["Distance_Emission_Factor"]/1000)
        b1=b1.assign(mileageRd=(b1["olometerFinal"]-b1["olometerInit"])*1000)
        b1.loc[(b1["tripMilageValue"]==0)&(b1["mileageRd"]>0),"tripMilageValue"]=b1["mileageRd"]
        b1.loc[(b1["tripMilageValue"]>500000)&(b1["mileageRd"]>0),"tripMilageValue"]=b1["mileageRd"]
		
        b2=b1.groupby("uid")[["tripMilageValue"]].sum()

        return b2, df_trip

    
    def idling_time_calc(self,df_eh):
        
        #calculate idling time
        c1=df_eh.groupby("uid")["idlingSec"].sum()

        return c1
    
    
    def descriptive_stat(self,a2,b2,c1,df_trip):

        res=pd.concat([a2,b2,c1],axis=1)
        #res=res.assign(Emission_Variance=res["Fuel_Emission"]-res["Dist_Emission"])
        res=res.assign(AvgConsumed=res["Fuel_Consumption_All"]/res["tripMilageValue"])


        res=res.reset_index()

        #x=res.loc[res["Fuel_Emission"]>0]

        vinfo=df_trip.groupby("uid").head(1)

        res2=pd.merge(res,vinfo[["uid","dt","Fuel"]],on="uid",how="left")
        #new on 23/01/2024
        ########res2=res2.loc[res2["dt"].notnull()]
        res2=res2.replace({np.nan: None})
        
        return res2
    
    
    def check_90day(self,cred,df_today):
        
        input = DatabaseExtraction()
        
        df_ref=input.extract_90day(cred)

        a1=df_ref.groupby("uid")[["Fuel_Consumption_All","tripMilageValue"]].sum()
        a1=a1.assign(AvgConsumed_90=a1["Fuel_Consumption_All"]/a1["tripMilageValue"])
        a1=a1.reset_index()

        b1=pd.merge(df_today,a1[["uid","AvgConsumed_90"]],on="uid",how='left')
        b1.loc[b1["AvgConsumed"].isnull(),"AvgConsumed"]=b1["AvgConsumed_90"]
        b1["AvgConsumed"]=b1["AvgConsumed"].astype(float)

        b1.loc[b1["AvgConsumed_90"]>0,"avgdiff"]=abs(b1["AvgConsumed_90"]-b1["AvgConsumed"])/b1["AvgConsumed_90"]
        
        b1.loc[b1["avgdiff"]>0.1,"AvgConsumed"]=b1["AvgConsumed_90"]
        b1=b1.assign(remark=np.nan)
        b1.loc[b1["tripMilageValue"]>800000,"remark"]='90day'
        b1.loc[b1["Fuel_Consumption_All"]>500,"remark"]='90day'
        b1.loc[(b1["Fuel_Consumption_All"]==0)&(b1["tripMilageValue"]>5000),"remark"]='90day'
        b1.loc[(b1["Fuel_Consumption_All"]>10)&(b1["tripMilageValue"]==0),"remark"]='90day'

        b1=b1.drop(columns=["AvgConsumed_90","avgdiff"])
        b1=b1.replace({np.nan: None})
        
        b1['Fuel']=b1['Fuel'].replace({'nan': None})   ##### newly added
        
        return b1
