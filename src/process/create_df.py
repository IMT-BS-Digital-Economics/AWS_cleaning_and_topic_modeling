#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 26/09/2023
    About: The goal of this loop is to attribute a part of the db to a thread to execute cleaning and modeling informations

"""
from multiprocess import Process, Manager

import pandas as pd

from src.process.clean_emails import clean_df
from src.utils.env_handle import get_env_var


def get_df_from_bucket(settings, bucket_uri):
    files_in_bucket = settings.get('awsDf').get_s3_bucket_obj_list(bucket_uri)

    procs = []

    max_process_number = get_env_var('MAX_THREAD', 'int')

    manager = Manager()

    frames = manager.list()

    for idx, file_uri in enumerate(files_in_bucket):
        if len(procs) == max_process_number:
            for proc in procs:
                proc.join()

            procs = []

        p = Process(target=clean_df, args=(frames, file_uri, settings.get('awsDf'),))

        p.start()

        procs.append(p)

    if procs:
        for proc in procs:
            proc.join()

    return pd.concat(frames, ignore_index=True)
