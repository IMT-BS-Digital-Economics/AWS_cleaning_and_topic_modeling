#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 26/09/2023
    About: The goal of this loop is to attribute a part of the db to a thread to execute cleaning and modeling informations

"""

from multiprocess import Process

from time import time

from src.utils.env_handle import get_env_var

from src.process.thread import thread_process


def seconds_to_hms(total_seconds):
    hours = int(total_seconds // 3600)
    total_seconds %= 3600
    minutes = int(total_seconds // 60)
    seconds = total_seconds % 60
    return f"{hours}H:{minutes}M:{int(seconds)}S"


def get_time_to_go(start_time, max_process_number, table_size, offset):
    data_processed_per_operation = (max_process_number - 1)

    execution_time_of_one_operation = time() - start_time

    data_left_to_be_processed = table_size - offset

    nb_of_operations_to_go = data_left_to_be_processed / data_processed_per_operation

    total_second = nb_of_operations_to_go * execution_time_of_one_operation

    return seconds_to_hms(total_second)


def loop(aws_df, aws_comprehend, mode, bucket_uri):
    procs = []

    max_process_number = get_env_var('MAX_THREAD', 'int')

    start_time = time()

    time_to_go = "Unknow"

    files_in_bucket = aws_df.get_s3_bucket_obj_list(bucket_uri)

    for idx, file_uri in enumerate(files_in_bucket):
        print(f'\rTime remaining: ~{time_to_go}', flush=True, end='')

        if len(procs) == max_process_number:
            for proc in procs:
                proc.join()

            time_to_go = get_time_to_go(start_time, max_process_number, len(files_in_bucket), idx)

            procs = []

        proc = Process(target=thread_process, args=(aws_df, aws_comprehend, file_uri, mode))

        proc.start()

        procs.append(proc)

    print('\nDone :)')
