
import json
import traceback
import sys
import datetime
import logging

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
                    logger.error(traceback.format_exc())
                    sys.exit(1)
                #except Exception as e:
                #    logger.error(f'Fail to extract attachments from Email. Error Message: {e.__class__}{e}')
                #    sys.exit(1)
        return fn, len(fn)


    def download_rpt(self,download_folder,mail,sender):
        #receive exception first
        msg1 = mail.messages(subject='User Login Report (CDAS)',date__on=datetime.date.today(),sent_from=sender,unread=True)

        fn1,n1=self.extract_msg(mail,msg1,download_folder)
        if n1==0:
            logger.error("There is no Login Report today!")
            sys.exit(1)
        elif n1!=1:
            logger.error("More than 1 Report today!")
            sys.exit(1)
        return fn1,n1