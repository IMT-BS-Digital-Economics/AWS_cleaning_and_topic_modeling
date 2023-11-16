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


def clean_row(row, column):
    with catch_warnings():
        simplefilter("ignore")

        text = BeautifulSoup(str(row[get_env_var(column, 'str')]), 'lxml').get_text().replace('\n', ' ')

        text = sub(r"(@\[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", text)

        text = text.strip()

        text = "".join(['\n' + char if char.isupper() else char for char in text])

        text = text.lower()

        return clean_text(text)


def clean_db(df, column):
    if df is None:
        raise Exception(f"No df succeed")

    column_name = get_env_var(column, 'str')

    if column_name not in df.columns:
        raise Exception(f"Please make sure you have a {column_name} column in your df: {df.columns}")

    df[f'cleaned_{column.lower()}'] = df.apply(lambda row: clean_row(row, column), axis=1)

    return df


def clean_df(file_uri, aws_df, column):
    df = aws_df.download_df_from_s3(file_uri)

    return clean_db(df, column)


def clean_process(file_uri, aws_df, process_name, start_time, column):
    try:
        df = clean_df(file_uri, aws_df, column)
    except Exception as e:
        write_thread_logs(process_name, f"Exception raised during emails cleaning: {format_exc()}")
        return

    write_thread_logs(process_name, f"Emails are cleaned, it took {int(time() - start_time)}s !")

    upload_time = time()

    upload_response = None

    dfs_to_upload = [{'df': df[[f'cleaned_{column.lower()}']], 'name': f'cleaned_column_{process_name}'}, {'df': df, 'name': f'cleaned_{process_name}'}]

    for df in dfs_to_upload:
        try:
            upload_response = aws_df.upload_to_s3(df.get('df'), get_env_var("AWS_TMP_BUCKET", "str"), df.get('name'))

            if 'paths' not in upload_response:
                write_thread_logs(process_name, 'No path found to use as input for AWS comprehend')
                return None

            write_thread_logs(process_name,
                              f"Df has been uploaded in {int(time() - upload_time)}s ! AWS response: {upload_response}")

        except Exception as e:
            write_thread_logs(process_name, f"Exception raised during upload of {df.get('name')}: {format_exc()}")

    return upload_response['paths'][0]
