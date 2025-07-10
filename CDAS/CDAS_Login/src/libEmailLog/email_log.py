import ast
import sys
import traceback

from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import smtplib
import os
import datetime
import json

import logging.config
logging.config.fileConfig('logging.ini',disable_existing_loggers=False)
logger = logging.getLogger(__name__)

class EmailLog:
    
    def __init__(self):
            
        with open("config/email_login.json") as f: 
           
            data = json.load(f)["EMAIL_ALERT"]
        
        self.server=data.get('server')
        self.port=data.get('port')
        self.email_server=data.get('email_server')
        self.password=data.get('password')
        self.receiver_list= data.get('receiver_list')
        
    def login(self):
        
        try:
            # initialize connection to our
            # email server, we will use gmail here
            smtp = smtplib.SMTP(self.server, self.port)
            smtp.ehlo()
            smtp.starttls()

            # Login with your email and password
            smtp.login(self.email_server, self.password)
            
            return smtp

        except smtplib.SMTPException as e:
            error_code = e.smtp_code
            error_message = e.smtp_error
            logger.error(f'Fail to Connect to Email Server. Error Code: {error_code}. Error Message: {error_message}')
            
            
    def message(self,subject,text, img,attachment):
        
        try :
            # build message contents
            msg = MIMEMultipart()
            msg['Subject'] = subject   # add in the subject
            msg.attach(MIMEText(text))  # add text contents

            # check if we have anything given in the img parameter
            if img is not None:
                # if we do, we want to iterate through the images, so let's check that
                # what we have is actually a list
                if type(img) is not list:
                    img = [img]  # if it isn't a list, make it one
                # now iterate through our list
                for one_img in img:
                    img_data = open(one_img, 'rb').read()  # read the image binary data
                    # attach the image data to MIMEMultipart using MIMEImage, we add
                    # the given filename use os.basename
                    msg.attach(MIMEImage(img_data, name=os.path.basename(one_img)))
            
            # we do the same for attachments as we did for images
            if attachment is not None:
                if type(attachment) is not list:
                    attachment = [attachment]  # if it isn't a list, make it one
                for one_attachment in attachment:
                    with open(one_attachment, 'rb') as f:
                        # read in the attachment using MIMEApplication
                        file = MIMEApplication(
                            f.read(),
                            name=os.path.basename(one_attachment)
                        )
                    # here we edit the attached file metadata
                    file['Content-Disposition'] = f'attachment; filename="{os.path.basename(one_attachment)}"'
                    msg.attach(file)  # finally, add the attachment to our message object
                
            return msg
        
        except Exception as e:
            logger.error(f'Error in Getting the Subject/Text/Image/Attachment. Error Message: {e.__class__}{e}')
        
    def sent(self,msg,smtp):
        
        try:
            smtp.sendmail(from_addr=self.email_server,to_addrs=self.receiver_list, msg=msg.as_string())
            
        except smtplib.SMTPException as e:
            error_code = e.smtp_code
            error_message = e.smtp_error
            logger.error(f'Fail to Send the Email. Error Code: {error_code}. Error Message: {error_message}')
            
        # Finally, don't forget to close the connection
        
        smtp.quit()


class LogFile():
    
    def __init__(self):
        
        self.logfile_path = 'logs/error_log.log'

        #self.latestlogfile_path = 'logs/latestlogfile.log'
    
    '''
	def line_checking(self):
        
        try:
            with open(self.logfile_path, 'r') as fp:
                num_lines = sum(1 for line in fp)
            fp.close()
            
            return num_lines
        
        except FileNotFoundError as e:
            
            logger.error(f'Error in Opening the File. Error Message: {e.__class__}{e}')
    '''    
    
    def read(self):
        try:
            logfile = open(self.logfile_path, "r")
            
            # only read last 100 lines
            lines = logfile.readlines()[-100:]
            dt_check=(datetime.date.today()-datetime.timedelta(0)).strftime('%Y-%m-%d')
            line_num=0
            last_lines=[]
            for line in lines:
                line_num += 1
                if dt_check in line and ('ERROR' in line or 'WARNING' in line):          
                    last_lines=lines[line_num-1:line_num+30]        # send the last 30 lines message

			
			#num_lines = self.line_checking()
            
            #if num_lines < 50:
                #last_lines = lines[-num_lines:]
            #else : 
                #last_lines = lines[-50:]
                
            logfile.close()
            
            return last_lines
        
        except Exception as e:
            
            logger.error(f'Error in Opening the File. Error Message: {e.__class__}{e}')
            
    '''
    def write(self):
        
        last_lines = self.read()
        
        try:
            # open file in write mode
            with open(self.latestlogfile_path, 'w') as fp:
                for item in last_lines:
                # write each item on a new line
                    fp.write("%s" % item)
            fp.close()

        except FileNotFoundError as e:
            
            logger.error(f'Error in Writing the File. Error Message: {e.__class__}{e}')
    '''    
	
def run():
    
    try:
        logfile = LogFile()
        
        #logfile.write()
        logger.info('Checking log files now.')
        last_lines=logfile.read()

        if last_lines==[]:
            logger.info('No error found today.')
        else:
		
            email = EmailLog()

            smtp = email.login()

            email_subject = 'Error in executing CDAS Login Algorithm'


            lines = ['Error in log file is found today. ']+last_lines
            email_text = '\t\n'.join(lines)

		
            email_img = None

            #email_attachment = 'logs/latestlogfile.log'
            
            email_attachment=None

            msg = email.message(subject = email_subject, text = email_text, img =  email_img, attachment = email_attachment)

            email.sent(msg,smtp)
        
    except Exception:
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        logging.shutdown()