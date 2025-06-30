import json
import boto3
import pandas as pd
import io
# import requests


def lambda_handler(event, context):
    # ----------- CONFIG -----------
    source_bucket = 'batch-sudheer'
    source_key = 'landing_zone/sales_data.csv'

    destination_bucket = 'batch-sudheer'
    destination_key = 'derived_zone/transformed_file.csv'

    # ----------- INIT CLIENT -----------
    s3_client = boto3.client('s3')

    # ----------- READ FROM S3 -----------
    response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
    csv_content = response['Body'].read().decode('utf-8')

    # Load into pandas DataFrame
    df = pd.read_csv(io.StringIO(csv_content))

    # ----------- TRANSFORM DATA -----------
    # Example: Keep only selected columns
    # df = df[['employee_id', 'name', 'status']]

    # ----------- UPLOAD TO DESTINATION S3 -----------
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=destination_bucket,
        Key=destination_key,
        Body=csv_buffer.getvalue()
    )

    print(f"✅ Transformed CSV uploaded to {destination_bucket}/{destination_key}")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            # "location": ip.text.replace("\n", "")
        }),
    }
