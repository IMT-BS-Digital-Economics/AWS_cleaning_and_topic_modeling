#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 26/09/2023
    About: Manage a thread operation to clean 39000 rows and process topic modeling

"""

from multiprocess import Process

from os import path

from time import time

from src.aws.comprehend import run_job
from src.process.analysis_merge import merge_process
from src.process.clean_emails import clean_process
from src.process.topic_modeling import topic_analysis_process
from src.utils.env_handle import get_env_var

from src.utils.logs import write_thread_logs


def get_process_name(file_uri):
    process_name = path.basename(file_uri)

    return path.splitext(process_name)[0].replace('.', '-')


def main_process(settings, file_uri, job_id, subject=False):
    start_time = time()

    output = None
    output_sentiment = None

    column = "TEXT" if not subject else "SUBJECT"

    process_name = f"{settings.get('name')}_{column.lower()}"

    write_thread_logs(process_name, f"file_uri: [{file_uri}]")

    bucket_result = get_env_var("AWS_RESULT_BUCKET", "str")

    if settings.get('mode') in ['cleaning', 'all']:
        write_thread_logs(process_name, "Now, we will clean emails")

        file_uri = clean_process(file_uri, settings.get('awsDf'), process_name, start_time, column)

    if settings.get('mode') in ['topic_analysis', 'all']:
        output = topic_analysis_process(file_uri, settings.get('awsComprehend'), settings.get('awsDf'), process_name, column.lower())

    if settings.get('mode') in ['sentiment', 'all'] and subject:
        job_output_uri = run_job(output.get('unique_col_input_uri'), settings.get('awsComprehend'), process_name, 'sentiment')

        output_sentiment = {'output_uri': job_output_uri, 'bucket_uri': bucket_result}

    if settings.get('mode') in ['merging', 'all', 'topic_analysis']:
        if output is None and job_id is not None:
            output_uri = run_job(file_uri, settings.get('awsComprehend'), process_name, 'topic', job_id=job_id)
            output = {'input_uri': file_uri, 'output_uri': output_uri, 'bucket_uri': bucket_result}

        merge_process(output, process_name, settings.get('awsDf'), output_sentiment)

    write_thread_logs(process_name, f"process has been finished in {int(time() - start_time)}s")


def thread_process(settings, file_uri, job_id=None):
    def first_thread():
        main_process(settings, file_uri, job_id)

    def second_thread():
        if settings.get('performOnSubject'):
            main_process(settings, file_uri, job_id, subject=True)

    thread1 = Process(target=first_thread)
    thread2 = Process(target=second_thread)

    thread1.start()
    thread2.start()