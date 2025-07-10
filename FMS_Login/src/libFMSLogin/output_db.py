
import sys
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
        except mariadb.Error as e:
            logger.error(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
        return conn


    def output_db_acc_login(self,a2,cred):
        try:
            conn=self.db_conn(self.login_cred(cred))

            cur=conn.cursor()

            sql1="INSERT INTO `FMS_Login_all` (`Username`,`dt`, `login_time`, `logout_time`,\
                `duration`, `login_count`, `Account`) VALUES (" + "%s,"*(len(a2.columns)-1) + "%s)"
            val1=list(a2.itertuples(index=False, name=None))
            cur.executemany(sql1, val1)
            conn.commit()


            conn.close()
            
            logger.info('FMS_Login_all table is successfully inserted.')
            
        except Exception as e:
                
            logger.error(f'Fail to insert data into FMS_Login_all table. Error Message: {e.__class__}{e}')
    
        
    #new added:    
    def output_db_acc_tree(self,a2,cred):
        
        try:
            
            conn=self.db_conn(self.login_cred(cred))

            cur=conn.cursor()
            
            cur.execute("truncate table FMS_account_tree")
            
            sql1="INSERT INTO `FMS_account_tree` (`Account1`, `Account2`, Accountname, `Username`)\
                VALUES (" + "%s,"*(len(a2.columns)-1) + "%s)"
            val1=list(a2.itertuples(index=False, name=None))
            cur.executemany(sql1, val1)
            conn.commit()


            conn.close()   
        
            logger.info('FMS_account_tree table is successfully inserted.')
            
        except Exception as e:
                
            logger.error(f'Fail to insert data into FMS_account_tree. Error Message: {e.__class__}{e}')
