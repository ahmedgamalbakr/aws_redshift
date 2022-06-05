#!/usr/bin/env python
# coding: utf-8

# In[1]:


import boto3
import pandas as pd
import psycopg2
import json


# In[2]:


import configparser 
config=configparser.ConfigParser()
config.read_file(open('cluster.config'))


# In[3]:


#------------------ cluster config ------------------------#

KEY                        =config.get('AWS','KEY')
SECRET                     =config.get("AWS","SECRET")
DWH_CLUSTER_TYPE           =config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES              =config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE              =config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER     =config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                     =config.get("DWH","DWH_DB")
DWH_DB_USER                 =config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD             =config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT                   =config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME           =config.get("DWH","DWH_IAM_ROLE_NAME")                       
                       
(KEY,SECRET,DWH_CLUSTER_TYPE) 


# In[4]:


pd.DataFrame({"Params":["DWH_CLUSTER_TYPE","DWH_NUM_NODES","DWH_NODE_TYPE","DWH_CLUSTER_IDENTIFIER","DWH_DB","DWH_DB_USER","DWH_DB_PASSWORD","DWH_PORT","DWH_IAM_ROLE_NAME"],
              "Value":[DWH_CLUSTER_TYPE,DWH_NUM_NODES,DWH_NODE_TYPE,DWH_CLUSTER_IDENTIFIER,DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT,DWH_IAM_ROLE_NAME]})


# In[5]:


#------------- create objects from resources-------------#

ec2=boto3.resource('ec2',
                   region_name="eu-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                  )


s3=boto3.resource('s3',
                  region_name="eu-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                 )



iam=boto3.client('iam',
                region_name="eu-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
               )



redshift=boto3.client('redshift',
                region_name="eu-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
               )


# In[6]:


#------------------------- show data in your S3 bucket-------------------------#

bucket=s3.Bucket("gamal-test-bucket")
log_data_files=[filename.key for filename in bucket.objects.filter(Prefix='')]
log_data_files


# In[7]:


roleArn=iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
roleArn


# In[8]:


#-----------------------create redshift cluster ------------------------------#

try:
    response = redshift.create_cluster(
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        
        # identifier & credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]
    
    )
except Exception as e:
    print(e)


# In[9]:


# ---------------------show the description for cluster------------------------# 
redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]


# In[10]:


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth',-1)
    keysToShow = ["ClusterIdentifier","NodeType","ClusterStatus","MasterUsername","DBName","Endpoint","VpcId"]
    x=[(k,v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x,columns=["Key","Value"])

my_cluster_props=redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(my_cluster_props)


# In[11]:


DWH_ENDPOINT=my_cluster_props['Endpoint']['Address']
DWH_ROLE_ARN= my_cluster_props['IamRoles'][0]['IamRoleArn']
DB_NAME=my_cluster_props['DBName']
DB_USER=my_cluster_props['MasterUsername']


# In[12]:


#-------Network_Vpc--------#
try:
   vpc=ec2.Vpc(id=my_cluster_props["VpcId"])
   defaultSg =list(vpc.security_groups.all())[0]
   print(defaultSg)


   defaultSg.authorize_ingress(
       GroupName=defaultSg.group_name,
       CidrIp='0.0.0.0/0',
       IpProtocol='TCP',
       FromPort=int(DWH_PORT),
       ToPort=int(DWH_PORT)
   )
except Exception as e:
   print(e)


# In[13]:


#----------Make connection to postgres --------------#
try:
    conn=psycopg2.connect(host=DWH_ENDPOINT,dbname=DB_NAME,user=DB_USER,password="Password123",port=5439)
except psycopg2.Error as e:
    print("Error: could not make connection to the postgress database")
    print(e)
conn.set_session(autocommit=True)


# In[14]:


try:
    cur=conn.cursor()
except psycopg2.Error as e:
    print("Error: could not get cursor to the database")
    print(e)
    


# In[15]:


# ---------------- Create Tables ---------------------#

try:
    cur.execute("""create table users(
    userid integer not null distkey sortkey,
    username char(8),
    firstname varchar(30),
    lastname varchar(30),
    city varchar(30),
    state char(2),
    email varchar(100),
    phone char(14),
    likesports boolean,
    liketheatre boolean,
    likeconcerts boolean,
    likejazz boolean,
    likeclassical boolean,
    likeopera boolean,
    likerock boolean,
    likevegas boolean,
    likebroadway boolean,
    likemusicals boolean);""")
except psycopg2.Error as e:
    print("Error:Issue creating table")
    print(e)
    


# In[16]:


try:
    cur.execute("""create table venue(
    venueid smallint not null distkey sortkey,
    venuename varchar(100),
    venuecity varchar(30),
    venuestate char(2),
    venueseats integer);""")
except psycopg2.Error as e:
    print("Error:Issue creating table")
    print(e)


# In[17]:


try:
    cur.execute("""create table category(
    catid smallint not null distkey sortkey,
    catgroup varchar(10),
    catname varchar(10),
    catdesc varchar(50));

create table date(
    dateid smallint not null distkey,
    caldate date not null,
    day character(3) not null,
    week smallint not null,
    month character(5) not null,
    qtr character(5) not null,
    year smallint not null,
    holiday boolean default('N'));
    
create table event(
   eventid integer not null distkey,
   venueid smallint not null,
   catid smallint not null,
   dateid smallint not null sortkey,
   eventname varchar(200),
   starttime timestamp);
   
create table listing(
    listid integer not null distkey,
    sellerid integer not null,
    eventid integer not null,
    dateid smallint not null sortkey,
    numtickets smallint not null,
    priceperticket decimal(8,2),
    totalprice decimal(8,2),
    listtime timestamp);

    
    """)
    
except psycopg2.Error as e:
    print("Error:Issue creating table")
    print(e)
    


# In[22]:


#-------------Copying Data From S3 to Tables In Redshift--------------------------#
try:
    cur.execute(""" 
    
    copy users from 's3://gamal-test-bucket/allusers_pipe.txt'
    credentials 'aws_iam_role=arn:aws:iam::995803013381:role/redshift-s3-access'
    delimiter '|'
    region 'eu-west-2' 
    
    """)
    
    
except psycopg2.Error as e:
    print("Error: issue copying data from s3 to table")
    print(e)


# In[23]:


try:
    cur.execute("""
    select * from users;
    
    """)
except psycopg2.Error as e:
    print(e)


# In[24]:


row=cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()
    


# In[ ]:


#----------------Delete Cluster ----------------------#

redshift.delete_clustere(clusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




