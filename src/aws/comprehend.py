#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 30/09/2023
    About: A cLass to handle comprehend functionalities

"""

from time import time, sleep

from boto3 import client

from src.utils.env_handle import get_env_var

from src.decorators.session_decorator import verify_session


def run_job(file_uri, aws_comprehend, process_name, job_type, job_id=None):
    if job_id is None:
        is_job_launch = aws_comprehend.create_job(file_uri, get_env_var('AWS_RESULT_BUCKET', 'str'), process_name,
                                                  job_type)

        if not is_job_launch:
            return None

    return aws_comprehend.wait_for_job_result(process_name, job_type, job_id)


class AwsComprehend:
    def __create_client(self):
        self.aws = client(
            service_name='comprehend',
            region_name=get_env_var('AWS_REGION_NAME', 'str'),
            aws_access_key_id=get_env_var('AWS_ACCESS_KEY_ID', 'str'),
            aws_secret_access_key=get_env_var('AWS_SECRET_ACCESS_KEY', 'str')
        )

    def __init__(self):
        self.session_time = time()

        self.__create_client()

        self.jobs = {}

        self.iam_user = get_env_var('AWS_IAM_USER', 'str')
        self.iam_role = get_env_var('AWS_IAM_ROLE', 'str')

    @verify_session(renew_session=__create_client)
    def create_topic_analysis_job(self, input_data_config, output_data_config, data_access_role_arn, job_name):
        number_of_topics = get_env_var('NUMBER_OF_TOPIC', 'int')

        job = self.aws.start_topics_detection_job(
            NumberOfTopics=number_of_topics,
            InputDataConfig=input_data_config,
            OutputDataConfig=output_data_config,
            DataAccessRoleArn=data_access_role_arn,
            JobName=job_name
        )

        self.jobs[job_name] = job

    @verify_session(renew_session=__create_client)
    def create_sentiment_analysis_job(self, input_data_config, output_data_config, data_access_role_arn, job_name):
        job = self.aws.start_sentiment_detection_job(
            InputDataConfig=input_data_config,
            OutputDataConfig=output_data_config,
            DataAccessRoleArn=data_access_role_arn,
            JobName=job_name,
            LanguageCode="en"
        )

        self.jobs[job_name] = job

    @verify_session(renew_session=__create_client)
    def create_job(self, input_path, output_path, job_name, job_type):
        data_access_role_arn = f"{self.iam_user}:{self.iam_role}"

        input_data_config = {"S3Uri": f"{input_path}", "InputFormat": "ONE_DOC_PER_LINE"}
        output_data_config = {"S3Uri": f"s3://{output_path}"}

        if job_type not in ['sentiment', 'topic']:
            return None

        analysis_job = {'sentiment': self.create_sentiment_analysis_job, 'topic': self.create_topic_analysis_job}

        analysis_job[job_type](input_data_config, output_data_config, data_access_role_arn, job_name)

        return 1

    def get_data_from_active_job(self, job_id, job_type):
        return self.aws.describe_sentiment_detection_job(
            JobId=job_id
        ) if job_type == "sentiment" else self.aws.describe_topics_detection_job(JobId=job_id)[
            'TopicsDetectionJobProperties']

    def get_job_progress_by_id(self, job_id, job_type, job_name):
        data = self.get_data_from_active_job(job_id, job_type)

        self.jobs[job_name] = data

        if 'JobStatus' in data:
            return data['JobStatus']

        return None

    @verify_session(renew_session=__create_client)
    def get_job_progress(self, job_name, job_type):
        my_job = self.jobs[job_name]

        if 'JobId' in my_job:
            data = self.get_data_from_active_job(my_job['JobId'], job_type)

            self.jobs[job_name] = data

            if 'JobStatus' in data:
                return data['JobStatus']

        return None

    @verify_session(renew_session=__create_client)
    def wait_for_job_result(self, job_name, job_type, job_id):
        while 1:
            job_status = self.get_job_progress(job_name, job_type) \
                if job_id is None \
                else self.get_job_progress_by_id(job_id, job_type, job_name)

            if not job_status or job_status in ['FAILED', 'STOP_REQUESTED', 'STOPPED']:
                return None

            if job_status == 'COMPLETED':
                return self.jobs[job_name]["OutputDataConfig"]["S3Uri"]

            sleep(5 * 60)
