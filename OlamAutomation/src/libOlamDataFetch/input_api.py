import datetime
import time
import requests
import sys
import pandas as pd
import dateutil
import logging
#import logging.config

logger = logging.getLogger(__name__)

from libUtil.access import ApiAccess


class ApiDataExtraction:

    
    def __init__(self,json_file,dt_use):
        
        access_token = ApiAccess().user_login(json_file)
        st_tm=datetime.datetime.strptime(dt_use+' 00:00:00', "%Y-%m-%d %H:%M:%S")
        st_tm=st_tm.replace(tzinfo=dateutil.tz.gettz('Singapore'))
        fromTm=str(int(st_tm.timestamp()*1000))
        toTm=str(int(st_tm.timestamp()+86400)*1000-1)
        print (fromTm)
        print (toTm)
		
        self.param = {'fromTm':fromTm,'toTm':toTm }
        self.header = { 'Authorization':'Bearer {}'.format(access_token) }
        
        self.url_event, self.url_patch=ApiAccess().url_link(json_file)
        
    def pallet_event_extract(self):

        response=requests.get(url=self.url_event,params=self.param,headers=self.header)
        
        if response.status_code==200:
            jsonResp=response.json()
            df_event=pd.json_normalize(response.json())
            #df_event=df_event[["createDt","pid","label","stateCode","pstat","eventCode","remark"]]
        else:
            logger.error("Error with Extracting Data from Pallet Event! Error Code " + str(response.status_code))
            sys.exit(1)

        return df_event
        
    def pallet_patch_silo_extract(self):
    
        response=requests.get(url=self.url_patch,params=self.param,headers=self.header)
        
        if response.status_code==200:
            jsonResp=response.json()
            df_patch=pd.json_normalize(response.json())
            
        else:
            logger.error("Error with Extracting Data from Pallet Patch Silo! Error Code " + str(response.status_code))
            sys.exit(1)
        
        return df_patch