#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 09/10/2023
    About: To merge result from analysis with existing df

"""

from os import system, path

from tarfile import open

from pandas import read_csv, concat

from src.utils.env_handle import get_env_var


def download_results(process_name, output, aws_df):
    work_path = f"./tmp/{process_name}"

    terms_filename = "topic-terms.csv"
    topics_filename = "doc-topics.csv"

    if not path.isdir(work_path):
        analysis_ouput_compress = f"{work_path}/output.tar.gz"

        system(f'mkdir {work_path}')

        if 'output_uri' not in output or 'bucket_uri' not in output:
            return None

        key = output['output_uri'][output['output_uri'].find(output['bucket_uri']) + len(output['bucket_uri']) + 1:]

        aws_df.download_file_using_client(output['bucket_uri'], key, analysis_ouput_compress)

        files = open(analysis_ouput_compress)

        files.extractall(work_path)

        if not path.isfile(f'{work_path}/{terms_filename}') or not path.isfile(f'{work_path}/{topics_filename}'):
            return None

    results = {'terms': read_csv(f'{work_path}/{terms_filename}'), 'topics': read_csv(f'{work_path}/{topics_filename}')}

    system(f"rm -rf {work_path}")

    return results


def get_analysis_df(process_name, output, aws_df):
    results = download_results(process_name, output, aws_df)

    if results is None:
        return

    df_topics = results['topics']
    df_terms = results['terms']

    df_topics = df_topics.shift(-1)

    df_topics['keywords'] = df_topics.apply(
        lambda row: list(df_terms[df_terms['topic'] == row['topic']]['term']) + [f"proportion: {row['proportion']}"],
        axis=1)

    df_topics = df_topics.groupby('docname', as_index=False).agg({'keywords': list})

    df_topics['lines'] = df_topics.apply(lambda row: row['docname'].split(':')[1], axis=1).astype(int)

    df_topics = df_topics.sort_values('lines').reset_index(drop=True)

    return df_topics


def merge_process(output, process_name, aws_df):
    if output is None:
        return

    df = aws_df.get_bucket_as_df(output['input_uri'])

    if df is None:
        return

    df_topics = get_analysis_df(process_name, output, aws_df)

    df = df.merge(df_topics, left_index=True, right_index=True)

    df = df[['unique_id', 'cleaned_text', 'keywords']]

    aws_df.upload_to_s3(df, get_env_var('AWS_STORAGE_BUCKET', 'str'), f"{process_name}_analytics")

