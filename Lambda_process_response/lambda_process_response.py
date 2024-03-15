

import os
import json
import boto3
import pandas as pd


def lambda_handler(event, context):

    BUCKET_NAME = os.environ["BUCKET_NAME"]
    PREFIX = os.environ["PREFIX"]

    job_id = json.loads(event["Records"][0]["Sns"]["Message"])["JobId"]

    page_lines = process_response(job_id)

    csv_key_name = f"{job_id}.csv"
    df = pd.DataFrame(page_lines.items())
    df.columns = ["PageNo", "Text"]
    
    comprehend_client = boto3.client('comprehendmedical')
    dynamodb = boto3.resource('dynamodb',region_name='us-east-1')  
    table = dynamodb.Table('medication_prescription') 
    name_list = []
    medication_list = []
   
    
    for value in df["Text"]:
        text_values_string = str(value)
        response =  comprehend_client.detect_entities_v2(Text=text_values_string)
        print(response['Entities'])
        
        resp = pd.DataFrame(response['Entities'])

        medication = resp[(resp['Category'] == 'MEDICATION')]['Text']
        name = resp[(resp['Type'] == 'NAME')]['Text']

        name_string = name.to_string(index=False) 
        medication_string = medication.to_string(index=False)
        
        # saving the output in a dynamodb table
        table.put_item(
            Item={
                'id': name_string,  # Adjust this if you have a different ID scheme
                'medication': medication_string
            }
        )
    
        name_list.append(name_string)
        medication_list.append(medication_string)

    # Create the DataFrame
    df1 = pd.DataFrame({'name': name_list, 'medication': medication_list})
    df1.to_csv(f"/tmp/{csv_key_name}", index=False)

    # saving the output in s3
    upload_to_s3(f"/tmp/{csv_key_name}", BUCKET_NAME, f"{PREFIX}/{csv_key_name}")
    # print(df)

    return {"statusCode": 200, "body": json.dumps("File uploaded successfully!")}


def upload_to_s3(filename, bucket, key):
    s3 = boto3.client("s3")
    s3.upload_file(Filename=filename, Bucket=bucket, Key=key)


def process_response(job_id):
    textract = boto3.client("textract")

    response = {}
    pages = []

    response = textract.get_document_text_detection(JobId=job_id)
    pages.append(response)
    
    
    nextToken = None
    if "NextToken" in response:
        nextToken = response["NextToken"]
    
    while nextToken:
        response = textract.get_document_text_detection(
            JobId=job_id, NextToken=nextToken
        )
        print("Response",response)
        pages.append(response)
        nextToken = None
        if "NextToken" in response:
            nextToken = response["NextToken"]

    page_lines = {}
    for page in pages:
        for item in page["Blocks"]:
            if item["BlockType"] == "LINE":
                if item["Page"] in page_lines.keys():
                    page_lines[item["Page"]].append(item["Text"])
                else:
                    page_lines[item["Page"]] = []

    
    return page_lines
    
    