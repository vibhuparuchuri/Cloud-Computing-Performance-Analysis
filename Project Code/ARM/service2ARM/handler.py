import json
import pymysql
import pandas as pd
import boto3
import io

import time


'''endpoint = 'database-1.cluster-cdtx8w7dz6yr.us-east-2.rds.amazonaws.com'
username = 'admintermproject'
password = 'admin123'
database_name = 'sales'
connection = pymysql.connect(host=endpoint,user=username,password=password,database=database_name)'''


def yourFunction(event, context):
    
    from Inspector import Inspector
    import logging
    import json

    inspector = Inspector()
    inspector.inspectAll()
    
    endpoint = 'database-1.cluster-cdtx8w7dz6yr.us-east-2.rds.amazonaws.com'
    username = 'admintermproject'
    password = 'admin123'
    database_name = 'sales'
    connection = pymysql.connect(host=endpoint,user=username,password=password,database=database_name)
    


    s3_client = boto3.client('s3')
    bucketname = event['bucketname']
    filename = event['filename']

    obj = s3_client.get_object(Bucket=bucketname, Key=filename+".csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    

    tablename ="sales"+ filename.split()[0]
    
    cursor = connection.cursor()
    
    

    statement = "Create table if not exists "+ tablename+ " ( "
    for x in range(1,len(df.columns)):
        if x!=1:
            statement=statement+" , "
        if df.dtypes[x]== "object":
            statement += str(df.columns[x]).replace(" ","_")+ " varchar(20)"
        else:
            statement += str(df.columns[x]).replace(" ","_")+ " int"

    statement=statement+ " );"
    cursor.execute(statement)
    
    cursor.execute("truncate table {0};".format(tablename.replace('\'', '\'\'')))
    
    
    
    df = df.iloc[: , 1:]
    sqlstatement = 'INSERT INTO ' + tablename + ' ' + '(Region,Country,Item_Type,Sales_Channel,Order_Priority,Order_Date,Order_ID,Ship_Date,Units_Sold,Unit_Price,Unit_Cost,Total_Revenue,Total_Cost,Total_Profit,Order_Processing_Time,Gross_Margin) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    
    cursor.executemany(sqlstatement,df.values.tolist())
    
    connection.commit()

    inspector.addAttribute("select1","Country")
    inspector.addAttribute("select2","AVG(Unit_Price)")
    inspector.addAttribute("group","Country")
    inspector.addAttribute("tablename",tablename)
    
    inspector.inspectAllDeltas()
    
    connection.close()
    
    return inspector.finish()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
