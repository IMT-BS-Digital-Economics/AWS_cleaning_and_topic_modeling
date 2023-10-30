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


def thread_process(aws_df, aws_comprehend, file_uri, mode, name, job_id=None):
    start_time = time()

    process_name = f"{name}"

    write_thread_logs(process_name, "Process is starting")

    output = None

    if mode == "cleaning" or mode == "all":
        write_thread_logs(process_name, "Now, we will clean emails")

        file_uri = clean_process(file_uri, aws_df, process_name, start_time)

    if mode == "topic_analysis" or mode == "all":
        output = analysis_process(file_uri, aws_comprehend, aws_df, process_name)

    if mode == "merging" or mode == "all" or mode == "topic_analysis":
        if output is None and job_id is not None:
            output = get_output_from_aws_comprehend_status(job_id, aws_comprehend, file_uri)

        merge_process(output, process_name, aws_df)

    write_thread_logs(process_name, f"process has been finished in {int(time() - start_time)}s")
