import os
import sys
import pandas as pd
import json
import mariadb
import logging
logger = logging.getLogger(__name__)


class Database:
    
    
    def login_cred(self,json_file):
        with open(json_file) as f: 
            data = json.load(f)['DATABASE']
            db_user,db_pw,db_host,db_port,database = data.get('db_user'), data.get('db_pw'), \
            data.get('db_host'), data.get('db_port'), data.get('database')
        return db_user,db_pw,db_host,db_port,database
        

    def db_conn(self,db_credential):
        try:
            db_user,db_pw,db_host,db_port,database=db_credential
            conn = mariadb.connect(
                user=db_user,
                password=db_pw,
                host=db_host,
                port=db_port,
                database=database
                )
            
            return conn
        
        except mariadb.Error as e:
            logger.error(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)


    def output_db(self,a2,cred):
        
        try:
            
            conn=self.db_conn(self.login_cred(cred))

            cur=conn.cursor()

            sql1="INSERT INTO `FMSLoginHist` (`User`, `Login_time`, `Logout_time`, `Duration`, `DurationSec`, `count`) VALUES (" + "%s,"*(len(a2.columns)-1) + "%s)"
            val1=list(a2.itertuples(index=False, name=None))
            cur.executemany(sql1, val1)
            conn.commit()
            conn.close()

            logger.info('FMSLoginHist table is successfully updated.')
            
        except Exception as e:
                
            logger.error(f'Fail to insert data into FMSLoginHist table. Error Message: {e.__class__}{e}')
    
    
    
    
    
