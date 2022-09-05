import json
import pymysql
import boto3
import io


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
    # TODO implement

    
    cursor = connection.cursor()
    tablename = event['tablename']

    select1 = event['select1']
    select2 = event['select2']
    groupby = event['group']
    
    #select Country, AVG(Unit_Price) from Sales1000 Group by Country;
    cursor.execute('select ' + select1 + ',' + ' ' + select2 + ' ' + 'from ' + tablename + ' ' + 'Group by ' + groupby)
    
    '''rows = cursor.fetchall()
    for row in rows:
        print("{0} {1}".format(row[0],row[1]))'''
        
    
    
    #connection.commit()
    connection.close()
    
    inspector.inspectAllDeltas()
    return inspector.finish()