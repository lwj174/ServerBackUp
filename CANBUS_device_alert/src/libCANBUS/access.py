import json
import mariadb
import sys

import logging
import logging.config
logger = logging.getLogger(__name__)

class DatabaseAccess:
    
    def at_login(self,json_file):
        with open(json_file) as f: 
            data = json.load(f)
            db_user,db_pw,db_host,db_port,database = data.get('user'), data.get('password'), \
            data.get('host'), data.get('port'), data.get('database')
        return db_user,db_pw,db_host,db_port,database


    def db_conn(self,json_cred):
        
        db_credential = self.at_login(json_cred)
        
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
    