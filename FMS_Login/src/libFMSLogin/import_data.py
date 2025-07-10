
import json
import traceback
import sys
import datetime
import logging
import os
import zipfile
import glob
from imbox import Imbox

logger = logging.getLogger(__name__)


class Email:
    
    def login_cred(self,json_file):
        with open(json_file) as f: 
            data = json.load(f)['EMAIL']
            username,password,host = data.get('user'), data.get('password'), data.get('imap_url')
            sender=data.get('sender')
        return username,password,host,sender


    def extract_msg(self,mail,messages,folder):
        fn=[]
        for (uid, message) in messages:
            mail.mark_seen(uid) # optional, mark message as read
            for idx, attachment in enumerate(message.attachments):
                try:
                    att_fn = attachment.get('filename')
                    att_fn = att_fn.replace("\r","")
                    att_fn = att_fn.replace("\n","")
                    fn.append(att_fn)
                    download_path = "{}/{}".format(folder, att_fn)
                    with open(download_path, "wb") as fp:
                        fp.write(attachment.get('content').read())
                except BaseException:
                    logger.error(traceback.print_exc())
                    sys.exit(1)
                except:
                    sys.exit(1)
        return fn, len(fn)


    def download_rpt(self,path,download_folder,mail,sender):
        #receive exception first
        msg1 = mail.messages(subject='User Login Report (ASCENT)',date__on=datetime.date.today()-datetime.timedelta(0),sent_from=sender)

        fn1,n1=self.extract_msg(mail,msg1,download_folder)
        if n1==0:
            logger.error("There is no Login Report today!")
            sys.exit(1)
        elif n1!=1:
            logger.warning("More than 1 Report today!")
            #sys.exit(1)
        return fn1,n1


    def get_login_data(self,download_folder,cred):
        
        if not os.path.isdir(download_folder):
            os.makedirs(download_folder, exist_ok=True)

        username,password,host,sender=self.login_cred(cred) 
        mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)

        fn1,n1=self.download_rpt(cred,download_folder,mail,sender)
        
        #os.chdir(download_folder)

        with zipfile.ZipFile(download_folder+'/'+fn1[0],"r") as zip_ref:
            zip_ref.extractall(download_folder)

        #os.chdir(download_folder+'/Ascent Support/xlsx')

        #path = os.getcwd()
        csv_files = glob.glob(os.path.join(download_folder+'/Ascent Support/xlsx', "*.xlsx"))
        return csv_files
