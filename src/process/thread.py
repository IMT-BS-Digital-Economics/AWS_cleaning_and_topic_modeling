#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 26/09/2023
    About: Manage a thread operation to clean 39000 rows and process topic modeling

"""

from src.process.clean_emails import clean_and_upload_df
from src.process.topic_modeling import run_topic_modeling


def thread_process(offset, aws_df, aws_comprehend, db, table, mode):
    process_name = f"df_{db}_{table}_topic_modeling_{offset}"

    upload_response = clean_and_upload_df(db, table, offset, aws_df, mode, process_name)

    if upload_response is not None:
        run_topic_modeling(upload_response, aws_comprehend, process_name)


    # Merge Df with JSONL response
    # Save DF to S3