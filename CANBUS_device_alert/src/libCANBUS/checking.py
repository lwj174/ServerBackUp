import pandas as pd
import logging

logger = logging.getLogger(__name__)

from libCANBUS.extraction import DatabaseExtraction

class DataChecking:
    
    #get fuel level check
    def fuel_check(self, cred, fuel_df, dt_latest):
        
        database_extraction = DatabaseExtraction()
        
        trip_df=database_extraction.get_trip(cred,fuel_df,dt_latest)
        t1=trip_df.assign(trip_mileage_km=trip_df["tripMilageValue"]/1000)
        t1=t1.assign(can_mileage=t1["olometerFinal"]-t1["olometerInit"])
        t1=t1.assign(final_mileage_km=t1["finalMilageValue"]/1000)
        t1.insert(22, "trip_mileage_km", t1.pop("trip_mileage_km"))
        t1.insert(23, "final_mileage_km", t1.pop("final_mileage_km"))
        t1.loc[t1["can_mileage"]==0,"mileage_all"]=t1["trip_mileage_km"]
        t1.loc[t1["can_mileage"]!=0,"mileage_all"]=t1["can_mileage"]
        t1.rename(columns={"ID":"Trip_ID"},inplace=True)
        #scenario 1: fuel level=0
        s1=t1.assign(fuelmin=t1[["fuelLvlInit","fuelLvlFinal"]].min(axis=1))
        #adding fuelconsumedlevel
        s1=s1.assign(fuelConsmin=s1[["fuelConsumedInit","fuelConsumedFinal"]].min(axis=1))
        s1=s1.assign(fuelConsRd=s1["fuelConsumedFinal"]-s1["fuelConsumedInit"])
        s1=s1.assign(fuelConsumed_all=s1[["fuelConsRd","fuelConsumed"]].max(axis=1))
    
        s2=s1.loc[(s1["fuelmin"]==0)&(s1["fuelConsmin"]==0)]
        s2=s2.assign(remark='Fuel Parameter Reading is 0')
    
        can_dist=s1.groupby("uid").agg({"mileage_all":'sum',"fuelConsumed_all":'sum'}).reset_index()
        ss3=can_dist.loc[(can_dist["fuelConsumed_all"]==0)&(can_dist["mileage_all"]>40)]
        s3=s1.loc[s1.uid.isin(ss3.uid)]
        s3=s3.assign(remark='Fuel Consumption is incorrect')
    
        s_all=pd.concat([s2,s3.loc[~s3.index.isin(s2.index)]])
    
        #update 4 Dec 2023
        #scenario 3: mileage fail & consumption fail
        can_dist=s1.groupby("uid").agg({"mileage_all":'sum',"fuelConsumed_all":'sum'}).reset_index()
        gps_dist=database_extraction.get_gps_dist(cred,fuel_df,dt_latest)
        dist_all=pd.merge(can_dist,gps_dist,on="uid")
        s4=dist_all.loc[(dist_all["mileage_all"]==0)&(dist_all["fuelConsumed_all"]==0)&(dist_all["distance_new"]>15)]
        s5=s1.loc[s1.uid.isin(s4.uid)]
        s5=s5.assign(remark='Mileage & Consumption Both Failed')
        s_all=pd.concat([s5,s_all.loc[~s_all.index.isin(s5.index)]])
    
        s6=dist_all.loc[abs(dist_all["distance_new"]-dist_all["mileage_all"])>50]
        s7=s1.loc[(s1.uid.isin(s6.uid))&(~(s1.uid.isin(s4.uid)))]
        s7=s7.loc[s7["mileage_all"]>500]
        s7=s7.assign(remark='Mileage Calculation Failed')
        s_all=pd.concat([s7,s_all.loc[~s_all.index.isin(s7.index)]])
    
    
        #get some statistics
        b1=t1.groupby("uid")["Trip_ID"].count().rename("total_trip_records")
        b2=s_all.groupby("uid")["Trip_ID"].count().rename("total_issue_records")
        a1=pd.merge(s_all,b1,on="uid")
        a2=pd.merge(a1,b2,on="uid")
        a3=pd.merge(a2,fuel_df[["uid","category","Device_Type","companyName"]],on="uid")

        a3.drop(columns=["beginTm","endTm","clientId","beginDtUtc","endDtUtc","speedAvg",
                     "initLink","finalLink","speedMax","finalMilage","finalMilageValue",
                     "initLat","initLng","finalLat","finalLng","mileage_all",
                     "createTm","tripMilage","tripMilageValue","driver","fuelmin",
                     "duration","batteryEnergyConsInit","batteryEnergyConsFinal",
                     "fuelConsmin","fuelConsRd","fuelConsumed_all",'accelPedalPosInit',
                     'accelPedalPosFinal','engineRPMInit', 'engineRPMFinal'],inplace=True)
        logger.info("Fuel check for "+dt_latest+" is done. Data added: "+str(len(a3)))
        
        return a3




    #get battery level check
    def battery_check(self,cred, battery_df, dt_latest):
        
        database_extraction = DatabaseExtraction()
        
        trip_df=database_extraction.get_trip(cred,battery_df,dt_latest)
        t1=trip_df.assign(trip_mileage_km=trip_df["tripMilageValue"]/1000)
        t1=t1.assign(final_mileage_km=t1["finalMilageValue"]/1000)
        t1=t1.assign(can_mileage=t1["olometerFinal"]-t1["olometerInit"])
        t1.insert(22, "trip_mileage_km", t1.pop("trip_mileage_km"))
        t1.insert(23, "final_mileage_km", t1.pop("final_mileage_km"))
        t1.loc[t1["can_mileage"]==0,"mileage_all"]=t1["trip_mileage_km"]
        t1.loc[t1["can_mileage"]!=0,"mileage_all"]=t1["can_mileage"]
        t1.rename(columns={"ID":"Trip_ID"},inplace=True)
        #scenario 1: battery level=0
        s1=t1.assign(batterymin=t1[["batteryLvlInit","batteryLvlFinal"]].min(axis=1))
        s1=s1.assign(batterymax=s1[["batteryLvlInit","batteryLvlFinal"]].max(axis=1))
        s1=s1.assign(batteryuse=s1["batteryLvlInit"]-s1["batteryLvlFinal"])
        s2=s1.loc[(s1["batterymin"]==0)&(s1["batterymax"]!=0)]
        s2=s2.assign(remark='Battery Level is 0')
        #scenario 2: battery consumption=0
        s3=s1.loc[(s1["batteryuse"]<=0)&(s1["mileage_all"]>10)]
        s3.uid.nunique()    
        #5km:0
        s3=s3.assign(remark='Battery Consumption is 0')
        
        s_all=pd.concat([s2,s3.loc[~s3.index.isin(s2.index)]])
		
        can_dist=s1.groupby("uid").agg({"mileage_all":'sum',"batteryuse":'sum'}).reset_index()
        gps_dist=database_extraction.get_gps_dist(cred,battery_df,dt_latest)
        dist_all=pd.merge(can_dist,gps_dist,on="uid")
        s4=dist_all.loc[(dist_all["mileage_all"]==0)&(dist_all["batteryuse"]==0)&(dist_all["distance_new"]>15)]
        s5=s1.loc[s1.uid.isin(s4.uid)]
        s5=s5.assign(remark='Mileage & Consumption Both Failed')
        s_all=pd.concat([s5,s_all.loc[~s_all.index.isin(s5.index)]])
    
        s6=dist_all.loc[abs(dist_all["distance_new"]-dist_all["mileage_all"])>50]
        s7=s1.loc[(s1.uid.isin(s6.uid))&(~(s1.uid.isin(s4.uid)))]
        s7=s7.loc[s7["mileage_all"]>500]
        s7=s7.assign(remark='Mileage Calculation Failed')
        s_all=pd.concat([s7,s_all.loc[~s_all.index.isin(s7.index)]])
		
		
		
        #get some statistics
        b1=t1.groupby("uid")["Trip_ID"].count().rename("total_trip_records")
        b2=s_all.groupby("uid")["Trip_ID"].count().rename("total_issue_records")
        a1=pd.merge(s_all,b1,on="uid")
        a2=pd.merge(a1,b2,on="uid")
        a3=pd.merge(a2,battery_df[["uid","category","Device_Type","companyName"]],on="uid")

        a3.drop(columns=["beginTm","endTm","clientId","beginDtUtc","endDtUtc","speedAvg",
                     "initLink","finalLink","speedMax","finalMilage","finalMilageValue",                     
                     "initLat","initLng","finalLat","finalLng","mileage_all",
                     "createTm","tripMilage","tripMilageValue","driver","duration",
                     "batterymin","batteryuse","batterymax","batteryEnergyConsInit","batteryEnergyConsFinal",
                     'accelPedalPosInit','accelPedalPosFinal','engineRPMInit', 'engineRPMFinal'],inplace=True)
        
        logger.info("Battery check for "+dt_latest+" is done. Data added: "+str(len(a3)))
        
        return a3    