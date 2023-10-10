#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 25/09/2023
    About: A bunch of functions to clean emails

"""

from time import time

from traceback import format_exc

from warnings import catch_warnings, simplefilter

from re import sub

from bs4 import BeautifulSoup

from src.utils.env_handle import get_env_var
from src.utils.logs import write_thread_logs


def clean_text(text):
    for element in [u'\xa0', '\n', '\r']:
        text = text.replace(element, '')

    return text


def clean_row(row):
    with catch_warnings():
        simplefilter("ignore")

        text = BeautifulSoup(str(row['body']), 'lxml').get_text().replace('\n', ' ')

        text = sub(r"(@\[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", text)

        text = text.strip()

        text = "".join(['\n' + char if char.isupper() else char for char in text])

        text = text.lower()

        return clean_text(text)


def clean_db(df):
    if df is None:
        raise Exception(f"No df succeed")

    if 'body' not in df.columns:
        raise Exception(f"Please make sure you have a body column in your df")

    df['cleaned_text'] = df.apply(lambda row: clean_row(row), axis=1)

    return df


def clean_df(file_uri, aws_df):
    df = aws_df.get_bucket_as_df(file_uri)

    return clean_db(df)


def clean_process(file_uri, aws_df, process_name, start_time):
    try:
        df = clean_df(file_uri, aws_df)
    except Exception as e:
        write_thread_logs(process_name, f"Exception raised during emails cleaning: {format_exc()}")
        return

    write_thread_logs(process_name, f"Emails are cleaned, it took {int(time() - start_time)}s !")

    upload_time = time()

    try:
        upload_response = aws_df.upload_to_s3(df, get_env_var("AWS_STORAGE_BUCKET", "str"), process_name)
    except Exception as e:
        write_thread_logs(process_name, f"Exception raised during upload of cleaned emails: {format_exc()}")
        return

    if 'paths' not in upload_response:
        write_thread_logs(process_name, 'No path found to use as input for AWS comprehend')
        return None

    write_thread_logs(process_name, f"Df has been uploaded in {int(time() - upload_time)}s ! AWS response: {upload_response}")

    return upload_response.paths[0]
