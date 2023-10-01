#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 30/09/2023
    About: A cLass to handle comprehend functionalities

"""

from time import time

from boto3 import client

from json import dumps

from src.utils.env_handle import get_env_var

from src.decorators.session_decorator import verify_session


class AwsComprehend:
    def __create_client(self):
        return client(
            service_name='comprehend',
            region_name=get_env_var('AWS_REGION_NAME', 'str'),
            aws_access_key_id=get_env_var('AWS_ACCESS_KEY_ID', 'str'),
            aws_secret_access_key=get_env_var('AWS_SECRET_ACCESS_KEY', 'str')
        )

    def __init__(self):
        self.session_time = time()

        self.aws = self.__create_client()

        self.jobs = {}

        self.iam_user = get_env_var('AWS_IAM_USER', 'str')
        self.iam_role = get_env_var('AWS_IAM_ROLE', 'str')

    @verify_session(renew_session=__create_client)
    def create_topic_analysis_job(self, input_path, output_path, job_name):
        data_access_role_arn = f"{self.iam_user}:{self.iam_role}"
        number_of_topics = get_env_var('NUMBER_OF_TOPIC', 'int')

        input_data_config = {"S3Uri": f"{input_path}", "InputFormat": "ONE_DOC_PER_FILE"}
        output_data_config = {"S3Uri": f"s3://{output_path}"}

        job = self.aws.start_topics_detection_job(
            NumberOfTopics=number_of_topics,
            InputDataConfig=input_data_config,
            OutputDataConfig=output_data_config,
            DataAccessRoleArn=data_access_role_arn,
            JobName=job_name
        )

        self.jobs[job_name] = dumps(job)

    @verify_session(renew_session=__create_client)
    def get_job_progress(self, job_name):
        return self.aws.describe_topics_detection_job(JobId=self.jobs[job_name]['JobId'])

    @verify_session(renew_session=__create_client)
    def get_job_results(self, job_name):
        return self.aws.list_topics_detection_jobs(
            Filter={
                'JobName': f'{job_name}',
            }
        )
