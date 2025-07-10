import requests
import sys
import json
import mariadb
import os
#os.chdir("D:/2021/PythonCodes/Olam Automation")
import logging
#import logging.config

logger = logging.getLogger(__name__)

class ApiAccess:    
    
    def login_info(self, json_file):
        
        with open(json_file) as f: 
            data = json.load(f)
            user, pw, loginURL = data.get('login'), data.get('password'), data.get('url_login')
        return user, pw, loginURL
    
    
    def user_login(self, json_file):
        
        user, pw, loginURL=self.login_info(json_file)
        payload = {"login":user,"password":pw}
        r=requests.post(url=loginURL, json=payload)

        status=r.status_code

        if status==200:

            jsonR=r.json()

            access_token=jsonR["data"]["token"]["accessToken"]

        else:
            logger.error("Error with Login! Error Code "+str(status))
            sys.exit(1)
        
        return access_token

    def url_link(self,json_file):
        with open(json_file) as f: 
            data = json.load(f)
            url1, url2 = data.get('url_data'), data.get('url_data2')
        return url1, url2
    

class DatabaseAccess:
    
    def login_info(self, json_file):
        
        with open(json_file) as f: 
            data = json.load(f)
            db_user,db_pw,db_host,db_port,database = data.get('db_user'), data.get('db_pw'), \
            data.get('db_host'), data.get('db_port'), data.get('database')
        return db_user,db_pw,db_host,db_port,database    


    def db_conn(self, json_file):
        
        try:
            db_user,db_pw,db_host,db_port,database=self.login_info(json_file)
            conn = mariadb.connect(
                user=db_user,
                password=db_pw,
                host=db_host,
                port=db_port,
                database=database
                )
        except mariadb.Error as e:
            logger.error(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
        return conn    
