#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 03/10/2023
    About: To handling uploads from df

"""

from src.utils.env_handle import get_env_var


def upload(aws_df, df, mode, process_name):
    if mode == "topic analysis":
        return aws_df.upload_to_s3(df, get_env_var('AWS_TMP_BUCKET', 'str'), process_name)
    else:
        return aws_df.upload_to_s3(df, get_env_var('AWS_STORAGE_BUCKET', 'str'), process_name)