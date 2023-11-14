#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 26/09/2023
    About: Manage a thread operation to clean 39000 rows and process topic modeling

"""

from os import path

from time import time

from src.aws.comprehend import get_output_from_aws_comprehend_status
from src.process.analysis_merge import merge_process
from src.process.clean_emails import clean_process
from src.process.topic_modeling import analysis_process

from src.utils.logs import write_thread_logs


def get_process_name(file_uri):
    process_name = path.basename(file_uri)

    return path.splitext(process_name)[0].replace('.', '-')


def main_process(settings, file_uri, job_id, subject=False):
    start_time = time()

    output = None

    column = "TEXT" if not subject else "SUBJECT"

    process_name = f"{settings.get('name')}_{column.lower()}"

    if settings.get('mode') in ['cleaning', 'all']:
        write_thread_logs(process_name, "Now, we will clean emails")

        file_uri = clean_process(file_uri, settings.get('awsDf'), process_name, start_time, column)

    if settings.get('mode') in ['topic_analysis', 'all']:
        output = analysis_process(file_uri, settings.get('awsComprehend'), settings.get('awsDf'), process_name)

    if settings.get('mode') in ['merging', 'all', 'topic_analysis']:
        if output is None and job_id is not None:
            output = get_output_from_aws_comprehend_status(job_id, settings.get('awsComprehend'), file_uri)

        merge_process(output, process_name, settings.get('awsDf'))

    write_thread_logs(process_name, f"process has been finished in {int(time() - start_time)}s")


def thread_process(settings, file_uri, job_id=None):
    main_process(settings, file_uri, job_id)

    if settings.get('performOnSubject'):
        main_process(settings, file_uri, job_id, subject=True)
