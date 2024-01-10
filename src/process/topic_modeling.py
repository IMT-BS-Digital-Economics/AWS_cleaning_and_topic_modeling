#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 30/09/2023
    About: To manage process topic modeling operations

"""

from src.utils.env_handle import get_env_var

from src.utils.logs import write_thread_logs

from src.aws.comprehend import run_job


def topic_analysis_process(df, aws_comprehend, aws_df, process_name, column):
    bucket = get_env_var("AWS_RESULT_BUCKET", "str")

    write_thread_logs(process_name, "Start analysis process :)")

    if f'cleaned_{column}' not in df:
        write_thread_logs(process_name, f"No column cleaned_{column} found :(")
        return None

    upload_response = aws_df.upload_to_s3(df[f'cleaned_{column}'], bucket, process_name,
                                          ext="csv")

    if 'paths' not in upload_response or not len(upload_response['paths']) > 0:
        return None

    unique_col_input_uri = upload_response['paths'][0]

    return {
        'input_uri': df,
        'unique_col_input_uri': unique_col_input_uri,
        'output_uri': run_job(unique_col_input_uri, aws_comprehend, process_name, 'topic'),
        'bucket_uri': bucket
    }
