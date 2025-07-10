#!/usr/bin/env python
# coding: utf-8

# In[1]:
import datetime
from libCDAS.main import main

if __name__ == '__main__':

    credential="config/email_login.json"
    fd =(datetime.datetime.now() - datetime.timedelta(1)).strftime('%b %d')
    main(fd,credential)


# In[ ]:




