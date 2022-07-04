
# coding: utf-8

# In[ ]:


import requests
from simple_salesforce import Salesforce, SFType, SalesforceLogin
import json
from pprint import pprint as pp
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO
import pandas as pd
from pandas.io.json import json_normalize 


# In[ ]:


def queryData(soql):
    result = sf.query(soql)
    print("Total Records: ", str(result['totalSize']))
    data = []
    isDone = result['done']
    if isDone:
        for rec in result['records']:
            rec.pop('attributes', None)
            data.append(rec)
        return data
    while not isDone:
        try:
            if not result['done']:
                for rec in result['records']:
                    rec.pop('attributes', None)
                    data.append(rec)
                result = sf.query_more(result['nextRecordsUrl'], True)
            else:
                for rec in result['records']:
                    rec.pop('attributes', None)
                    data.append(rec)
                    isDone = True
                print('Query Completed')
                
        except NameError:
                for rec in result['records']:
                    rec.pop('attributes', None)
                    data.append(rec)
                result = sf.query_more(result['nextRecordsUrl'], True)
        return data


# In[ ]:

#data from https://data.cms.gov/provider-data/dataset/mj5m-pzi6
hcpdata = pd.read_csv('hcp_acc_sample.csv')
hcpdata.shape


# In[ ]:


physdata = pd.read_csv('hcp_phys_sample.csv')
physdata.shape


# In[ ]:


#query source org accounts
# src_acc_soql = ("SELECT Id, Name, AccountNumber FROM Account ")
# src_acc_results = pd.DataFrame(queryData(src_acc_soql))
# print(src_acc_results.shape)


# In[ ]:


login = json.load(open('login_khtarget.json'))
username = login['username']
password = login['password']
token = login['token']
session_id, instance = SalesforceLogin(username=username, password=password, security_token=token, sandbox=False)
sf = Salesforce(instance=instance, session_id=session_id)
sf


# In[ ]:


#Create account data in target org
acc_bulk_data = []
for row in hcpdata.itertuples():
    d = row._asdict()
    del d['Index']
    acc_bulk_data.append(d)

sf.bulk.Account.insert(acc_bulk_data)


# In[ ]:


#query target org accounts
tar_acc_soql = ("SELECT Id, Name, AccountNumber FROM Account ")
tar_acc_results = pd.DataFrame(queryData(tar_acc_soql))
print(tar_acc_results.shape)


# In[ ]:


#query source org contacts
# cont_desc = sf.Contact.describe() 
# cont_field_names = [field['name'] for field in cont_desc['fields']]
# src_cont_soql = "SELECT {} FROM Contact ".format(','.join(cont_field_names))
# src_cont_results = pd.DataFrame(queryData(src_cont_soql))
# print(src_cont_results.shape)


# In[ ]:


tar_cols = ['LastName', 'FirstName', 'Description', 'MailingStreet', 'MailingCity', 'MailingState', 'MailingPostalCode', 'AccountNum__c', 'MailingCountry']
def getacctid(acctnum):
    if acctnum:
        if tar_acc_results[tar_acc_results['AccountNumber'] == str(acctnum)].size > 0:
            return tar_acc_results[tar_acc_results['AccountNumber'] == str(acctnum)]['Id'].values[0]
getacctid(50195)


# In[ ]:


physdata['MailingCountry'] = ['United States'] * physdata.shape[0]


# In[ ]:


physdata.columns = tar_cols
physdata['AccountId'] = physdata['AccountNum__c'].apply(getacctid)
physdata = physdata.drop(columns=['AccountNum__c'])
physdata


# In[ ]:


#Create target org contacts
cont_bulk_data = []
for row in physdata.itertuples():
    d = row._asdict()
    del d['Index']
    cont_bulk_data.append(d)
# cont_bulk_data
sf.bulk.Contact.insert(cont_bulk_data)

