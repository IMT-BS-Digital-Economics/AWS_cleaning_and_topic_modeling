#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 26/09/2023
    About: Manage a thread operation to clean 39000 rows and process topic modeling

"""

from time import time, sleep

from src.process.clean_emails import clean_and_upload_df
from src.process.topic_modeling import run_topic_modeling

from src.utils.logs import write_thread_logs


def thread_process(offset, aws_df, aws_comprehend, db, table, mode):
    start_time = time()

    process_name = f"df_{db}_{table}_{offset}"

    write_thread_logs(process_name, "process is starting")

    write_thread_logs(process_name, "Now, we will clean emails")

    upload_response = clean_and_upload_df(db, table, offset, aws_df, mode, process_name)

    write_thread_logs(process_name, "emails are cleaned!")

    if upload_response is not None:
        run_topic_modeling(upload_response, aws_comprehend, process_name)

    write_thread_logs(process_name, f"process has been finished in {int(time() - start_time)}s")

    # Merge Df with JSONL response
    # Save DF to S3