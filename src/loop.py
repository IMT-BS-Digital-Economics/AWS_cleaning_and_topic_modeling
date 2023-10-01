#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 26/09/2023
    About: The goal of this loop is to attribute a part of the db to a thread to execute cleaning and modeling informations

"""

import threading

from src.utils.env_handle import get_env_var

from src.process.thread import thread_process


def get_size_of_db(aws_df, db, table):
    df_size = aws_df.get_df_from_athena(f'SELECT COUNT("body") as column_count FROM {table};', db)

    if not 'column_count' in df_size.columns:
        raise Exception('Failed to get column count')

    df_dict = df_size.to_dict()['column_count']

    key = list(df_dict.keys())[0]

    return df_dict[key]


def loop(db, table, aws_df, aws_comprehend, mode):
    threads = []

    max_threads_number = get_env_var('MAX_THREAD', 'int')

    table_size = get_size_of_db(aws_df, 'csv-parsed', 'dataset2')

    offset = 0

    while offset + 39000 < table_size:
        print(f'\rNumbers of Threads in use: {threading.active_count()} - {offset}/{table_size}', flush=True, end='')

        if threading.active_count() == max_threads_number:
            for t in threads:
                t.join()

            threads = []

        t = threading.Thread(target=thread_process, args=(offset, aws_df, aws_comprehend, db, table, mode))

        t.start()

        threads.append(t)

        offset += 39000

    print("Done")
