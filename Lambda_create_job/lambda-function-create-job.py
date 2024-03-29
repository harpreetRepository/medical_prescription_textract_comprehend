import os
import json
import boto3


output_bucket_name = os.environ["output_bucket_name"]
output_s3_prefix = os.environ["output_s3_prefix"]
sns_topic_arn = os.environ["sns_topic_arn"]
sns_role_arn = os.environ["sns_role_arn"]


def lambda_handler(event, context):
    
    textract = boto3.client("textract")
    
    if event:
        file_obj = event["Records"][0]
        bucketname = str(file_obj["s3"]["bucket"]["name"])
        filename = unquote_plus(str(file_obj["s3"]["object"]["key"]))
        
        
        print(f"Bucket: {bucketname} ::: Key: {filename}")
        
        response = textract.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": bucketname, "Name": filename}},
            OutputConfig={"S3Bucket": OUTPUT_BUCKET_NAME, "S3Prefix": OUTPUT_S3_PREFIX},
            NotificationChannel={"SNSTopicArn": SNS_TOPIC_ARN, "RoleArn": SNS_ROLE_ARN},
        )
        
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return {"statusCode": 200, "body": json.dumps("Job created successfully!")}
        else:
            return {"statusCode": 500, "body": json.dumps("Job creation failed!")}
        