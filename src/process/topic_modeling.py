#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 30/09/2023
    About: To manage process topic modeling operations

"""

from time import sleep

from src.utils.env_handle import get_env_var

from src.utils.logs import write_thread_logs


def run_topic_modeling(file_uri, aws_comprehend, process_name):
    properties = 'TopicsDetectionJobPropertiesList'

    job_status = 'Not_Started'

    # Run AWS Comprehend request command to launch Topic Analysis

    aws_comprehend.create_topic_analysis_job(file_uri, get_env_var('AWS_RESULT_BUCKET', 'str'), process_name)

    while job_status != 'COMPLETED':
        res = aws_comprehend.get_job_progress(process_name)

        if 'TopicsDetectionJobProperties' in res and "JobStatus" in res['TopicsDetectionJobProperties']:
            job_status = res['TopicsDetectionJobProperties']["JobStatus"]

        if job_status != 'COMPLETED':
            sleep(5 * 60)

    # Get JSONL response
    response = aws_comprehend.get_job_results(process_name)

    if properties in response and len(response[properties]) > 0 and 'OutputDataConfig' in response[properties][
        0] and 'S3Uri' in response[properties][0]['OutputDataConfig']:
        return response[properties][0]['OutputDataConfig']['S3Uri']
    else:
        return None


def analysis_process(file_uri, aws_comprehend, aws_df, process_name):
    bucket = get_env_var("AWS_TMP_BUCKET", "str")

    if not file_uri:
        return

    write_thread_logs(process_name, "Start analysis process :)")

    df = aws_df.get_bucket_as_df(file_uri)

    if 'cleaned_text' not in df:
        write_thread_logs(process_name, "No column cleaned_text found :(")
        return None

    upload_response = aws_df.upload_to_s3(df['cleaned_text'], bucket, process_name,
                                          ext="csv")

    if 'paths' not in upload_response or not len(upload_response['paths']) > 0:
        return None

    file_uri = upload_response['paths'][0]

    return {'input_uri': file_uri, 'output_uri': run_topic_modeling(file_uri, aws_comprehend, process_name), 'output_bucket': bucket}
