#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 25/09/2023
    About: A bunch of functions to clean emails

"""

from warnings import catch_warnings, simplefilter

from re import sub

from bs4 import BeautifulSoup


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


def clean_df(db, table, offset, aws_df):
    limit = 39000

    # Get Df from AWS Athena
    df = aws_df.get_df_from_athena(f'SELECT "unique_id", "body" FROM "{db}"."{table}" OFFSET {offset} LIMIT {limit};',
                                   db)
    # Clean Df

    df = clean_db(df)

    return df
