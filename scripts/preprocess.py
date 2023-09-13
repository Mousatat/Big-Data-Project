import subprocess
command = 'pip install gdown'
output = subprocess.check_output(command, shell=True)
output_str = output.decode('utf-8')
print(output_str)

file_id = '1-QQylhU5U5rz2N7OP9ddDq0K_mqwrbwv'
output_file = 'data/bigdataproject.zip'

command = f"gdown --id {file_id} -O {output_file}"
output = subprocess.check_output(command, shell=True)
output_str = output.decode('utf-8')
print(output_str)

command = 'unzip data/bigdataproject.zip -d data/'
output = subprocess.check_output(command, shell=True)
output_str = output.decode('utf-8')
print(output_str)
#importing modules
import warnings 
warnings.filterwarnings('ignore')
import time
t = time.time()
print('Importing startred...')
import pandas as pd
from datetime import datetime
print('Done, All the required modules are imported. Time elapsed: {}sec'.format(time.time()-t))
# loading data
customer_df = pd.read_csv('data/customers.csv', delimiter = ',', encoding = 'utf-8')
pings_df = pd.read_csv('data/pings.csv', delimiter = ',', encoding = 'utf-8')
test_df = pd.read_csv('data/test.csv', delimiter = ',', encoding = 'utf-8')
### value sorting with respect to id and timestamp
pings_df = pings_df.sort_values(by = ['id','timestamp']).reset_index(drop=True)

# creating a copy to preserve actual ping data
temp_ping_df = pings_df.copy()

#pre-processing data
temp_ping_df.drop_duplicates(inplace = True)
temp_ping_df['timestamp_decode'] = temp_ping_df['timestamp'].apply(lambda x: datetime.fromtimestamp(x))


## timestamp and datagrouping
temp_ping_df['date'] = temp_ping_df['timestamp_decode'].dt.date
temp_ping_df['online_hours'] = (temp_ping_df.groupby(by=['id','date'])['timestamp'].diff())/(60*60)
temp_ping_df['online_hours']  =  temp_ping_df['online_hours'].apply(lambda x: x if x< (2/60) else (2/60))

temp_ping_df.fillna(0,inplace = True)

#### creating our training data
train_df= (temp_ping_df.groupby(by = ['id','date'])['online_hours'].sum()).reset_index()
train_df['online_hours'] = round(train_df['online_hours'],1)


## print statements
print('\n' +'*'*50+ '\n')

print('Head of the processed train data'+ '\n')
print(train_df.head())

print('\n' +'*'*50 + '\n')
## lets correct the training dataset...
train_df['date'] = train_df['date'].astype(str)
train_df =  train_df[train_df['date'] < '2017-06-22'] ## datecorrection

train_date_df = pd.DataFrame({'date':pd.date_range(start='2017-06-01', end='2017-06-21')}) ## data time series for correcting training data date
train_date_df['date'] = (train_date_df['date'].dt.date).astype(str)


## creating a grouped id, data dict 
ids_group = train_df.groupby(by ='id')['date'].unique()

##### creating a zero usage user dataframe to concatinate with training dataset

id_list = []
date_list = []
online_hours = []
for index, ids, dates in zip( range(len(ids_group)), ids_group.keys(), ids_group.values):
    
    date_ = dates.tolist()
    
    for date in train_date_df['date'].tolist():
        if date_ == train_date_df['date'].tolist():
            break
        else:
            if date not in date_:
                #tempdf[['id','date','online_hours'] ]= ids,date,0
                id_list.append(ids)
                date_list.append(date)
                online_hours.append(0)
                date_.append(date)

zero_df = pd.DataFrame({'id':id_list,'date':date_list, 'online_hours':online_hours})

train_df_backup = train_df.copy() # lets store away train_df for any furtue purpose

train_df = pd.concat([train_df,zero_df],sort = False)

train_df = train_df.sort_values(by = ['id','date'])
train_df.to_csv('data/train_new.csv', index=False)