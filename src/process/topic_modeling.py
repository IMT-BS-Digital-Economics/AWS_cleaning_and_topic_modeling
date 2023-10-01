#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 30/09/2023
    About: To manage process topic modeling operations

"""

from time import sleep

from src.utils.env_handle import get_env_var


def run_topic_modeling(upload_response, aws_comprehend, process_name):
    properties = 'TopicsDetectionJobPropertiesList'

    job_status = 'Not_Started'

    if 'paths' not in upload_response:
        raise Exception('No path found to use as input for AWS Comprehend')

    # Run AWS Comprehend request command to launch Topic Analysis

    aws_comprehend.create_topic_analysis_job(upload_response['paths'][0], get_env_var('AWS_RESULT_BUCKET', 'str'), process_name)

    while job_status != 'COMPLETED':
        res = aws_comprehend.get_job_progress(process_name)

        if 'TopicsDetectionJobProperties' in res and "JobStatus" in res['TopicsDetectionJobProperties']:
            job_status = res['TopicsDetectionJobProperties']["JobStatus"]

        if job_status != 'COMPLETED':
            sleep(5*60)

    # Get JSONL response
    response = aws_comprehend.get_job_results(process_name)

    if properties in response and len(response[properties]) > 0 and 'OutputDataConfig' in response[properties][0] and 'S3Uri' in response[properties][0]['OutputDataConfig']:
        return response[properties][0]['OutputDataConfig']['S3Uri']
    else:
        return None