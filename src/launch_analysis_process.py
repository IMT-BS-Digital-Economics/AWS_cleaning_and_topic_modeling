#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 09/01/2024
    About: 

"""
from time import time

from multiprocess import Process

from src.aws.comprehend import run_job
from src.process.analysis_merge import merge_process
from src.process.clean_emails import clean_process
from src.process.create_df import get_df_from_bucket
from src.process.topic_modeling import topic_analysis_process
from src.utils.env_handle import get_env_var


def analysis_process(settings, df, subject=False):
    start_time = time()

    output = None
    output_sentiment = None

    column = "TEXT" if not subject else "SUBJECT"

    bucket_result = get_env_var("AWS_RESULT_BUCKET", "str")

    process_name = f"{settings.get('name')}_{column.lower()}"

    upload_path_uri = clean_process(df, settings.get('awsDf'), process_name, start_time, column)

    output = topic_analysis_process(df, settings.get('awsComprehend'), settings.get('awsDf'), process_name, column.lower())

    if settings.get('mode') in ['sentiment', 'all'] and subject and output:
        job_output_uri = run_job(output.get('unique_col_input_uri'), settings.get('awsComprehend'), process_name, 'sentiment')

        output_sentiment = {'output_uri': job_output_uri, 'bucket_uri': bucket_result}

    merge_process(output, df, process_name, settings.get('awsDf'), output_sentiment, column)


def launch(settings, args):
    df = get_df_from_bucket(settings, args.get('bucketUri'))

    def first_thread():
        analysis_process(settings, df)

    def second_thread():
        if settings.get('performOnSubject'):
            analysis_process(settings, df, subject=True)

    thread1 = Process(target=first_thread)
    thread2 = Process(target=second_thread)

    thread1.start()
    thread2.start()