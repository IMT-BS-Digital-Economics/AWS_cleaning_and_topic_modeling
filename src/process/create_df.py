#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 26/09/2023
    About: The goal of this loop is to attribute a part of the db to a thread to execute cleaning and modeling informations

"""
import multiprocessing
from time import time

import pandas as pd

from src.process.clean_emails import clean_df
from src.utils.env_handle import get_env_var


def get_df_from_bucket(settings, bucket_uri):
    files_in_bucket = settings.get('awsDf').get_s3_bucket_obj_list(bucket_uri)

    df = None

    for idx, file_uri in enumerate(files_in_bucket):
        tmp_df = clean_df(file_uri, settings.get('awsDf'))

        print(f'Concat {tmp_df.shape}')

        if df is None:
            df = tmp_df
        else:
            df = pd.concat([df, tmp_df], ignore_index=True)

    print(f'{df.shape}')

    return df
