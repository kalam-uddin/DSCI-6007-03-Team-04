import boto3

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

glue_job_name = 'etl_glue_invoked_by_lambda2'

def lambda_handler(event, context):
    print("Event received:", event)
    
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        s3_path = f"s3://{bucket_name}/{key}"
        print(f"Starting Glue job for: {s3_path}")
        
        glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                "--S3_SOURCE_PATH": s3_path,
                "--S3_DESTINATION_PATH": "s3://term-project-target/glue_transformed_data/"
            }
        )
