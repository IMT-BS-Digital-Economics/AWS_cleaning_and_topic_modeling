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


def get_size_of_db(aws_df, db, table):
    df_size = aws_df.get_df_from_athena(f'SELECT COUNT("body") as column_count FROM {table};', db)

    if not 'column_count' in df_size.columns:
        raise Exception('Failed to get column count')

    df_dict = df_size.to_dict()['column_count']

    key = list(df_dict.keys())[0]

    return df_dict[key]


def seconds_to_hms(total_seconds):
    hours = int(total_seconds // 3600)
    total_seconds %= 3600
    minutes = int(total_seconds // 60)
    seconds = total_seconds % 60
    return f"{hours}H:{minutes}M:{int(seconds)}S"


def get_time_to_go(start_time, max_process_number, table_size, offset):
    data_processed_per_operation = 39000 * (max_process_number - 1)

    execution_time_of_one_operation = time() - start_time

    data_left_to_be_processed = table_size - offset

    nb_of_operations_to_go = data_left_to_be_processed / data_processed_per_operation

    total_second = nb_of_operations_to_go * execution_time_of_one_operation

    return seconds_to_hms(total_second)


def loop(db, table, aws_df, aws_comprehend, mode, offset=0):
    procs = []

    max_process_number = get_env_var('MAX_THREAD', 'int')

    table_size = get_size_of_db(aws_df, 'csv-parsed', 'dataset2')

    start_time = time()

    time_to_go = "Unknow"

    while offset + 39000 < table_size:
        print(f'\rTime remaining: ~{time_to_go}', flush=True, end='')

        if len(procs) == max_process_number:
            for proc in procs:
                proc.join()

            time_to_go = get_time_to_go(start_time, max_process_number, table_size, offset)
            procs = []

        proc = Process(target=thread_process, args=(offset, aws_df, aws_comprehend, db, table, mode))

        proc.start()

        procs.append(proc)

        offset += 39000

    print("Done")
