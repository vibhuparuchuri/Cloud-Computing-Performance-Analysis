#cloud_function(platforms=[Platform.AWS], memory=512, config=config)
import boto3
import pandas as pd
import io
import time

def yourFunction(request, context):
    import json
    import logging
    from Inspector import Inspector
    import time
    
    # Import the module and collect data 
    inspector = Inspector()
    inspector.inspectAll()
    bucketname = request['bucketname']
    filename = request['filename']
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucketname, Key=filename + ".csv")
    #Transformation 1
    initial_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    initial_df['Ship Date'] = pd.to_datetime(initial_df['Ship Date'])
    initial_df['Order Date'] = pd.to_datetime(initial_df['Order Date'])
    initial_df['Order Processing Time'] = initial_df['Ship Date'] - initial_df['Order Date']
    #Transformation 2
    initial_df.loc[initial_df['Order Priority'] == 'L','Order Priority'] = 'Low'
    initial_df.loc[initial_df['Order Priority'] == 'M', 'Order Priority'] = 'High'
    initial_df.loc[initial_df['Order Priority'] == 'H', 'Order Priority'] = 'Medium'
    initial_df.loc[initial_df['Order Priority'] == 'C', 'Order Priority'] = 'Critical'
    #Transformation 3
    initial_df['Gross Margin'] = initial_df['Total Profit'] / initial_df['Total Revenue']
    #Transformation 4
    initial_df.drop_duplicates(subset='Order ID', keep="first")
    
    csv_buffer = io.StringIO()
    initial_df.to_csv(csv_buffer)
    s3_client.put_object(Bucket=bucketname,Body=csv_buffer.getvalue(),Key=filename + "Transformed.csv")
    
    inspector.addAttribute("bucketname",bucketname)
    inspector.addAttribute("filename",filename + "Transformed")
    


    inspector.inspectAllDeltas()
    return inspector.finish()