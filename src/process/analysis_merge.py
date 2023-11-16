#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 09/10/2023
    About: To merge result from analysis with existing df

"""

from os import system, path

from tarfile import open

from pandas import read_csv

from src.utils.env_handle import get_env_var
from src.utils.logs import write_thread_logs
from src.utils.string_handling import replace_last


def download_results(process_name, output, aws_df):
    work_path = f"./tmp/{process_name}"

    terms_filename = "topic-terms.csv"
    topics_filename = "doc-topics.csv"

    analysis_ouput_compress = f"{work_path}/output.tar.gz"

    if not path.isdir(work_path):
        system(f'mkdir {work_path}')

    if 'output_uri' not in output or 'bucket_uri' not in output:
        return None

    output_uri = output['output_uri'].replace('s3://', '')

    file_name = output_uri[output_uri.find('/') + 1:]

        aws_df.download_file_using_client(output['bucket_uri'], key, analysis_ouput_compress)

    files = open(analysis_ouput_compress)

    files.extractall(work_path)

    if not path.isfile(f'{work_path}/{terms_filename}') or not path.isfile(f'{work_path}/{topics_filename}'):
        return None

    results = {'terms': read_csv(f'{work_path}/{terms_filename}'), 'topics': read_csv(f'{work_path}/{topics_filename}')}

    system(f"rm -rf {work_path}")

    return results


def set_proportion(row, topic):
    try:
        index = row['topic'].index(float(topic.replace('Topic ', '')))
    except ValueError:
        return 0

    return row['proportion'][index]


def get_analysis_df(process_name, output, aws_df):
    results = download_results(process_name, output, aws_df)

    if results is None:
        return None, None, None

    df_topics = results['topics']
    df_terms = results['terms']

    df_topics = df_topics.shift(-1)

    df_terms['topic'] = df_terms.apply(lambda row: 'Topic ' + str(row['topic']), axis=1)

    topic_list = df_terms.drop_duplicates(['topic'])['topic'].astype(str).tolist()

    df_terms_cpy = df_terms.groupby('topic', as_index=False).agg({'term': list, 'weight': list})

    df_topics = df_topics.groupby('docname', as_index=False).agg({'topic': list, 'proportion': list})

    df_topics['lines'] = df_topics.apply(lambda row: row['docname'].split(':')[1], axis=1).astype(int)

    df_topics = df_topics.sort_values('lines').reset_index(drop=True)

    for topic in topic_list:
        df_topics[topic] = df_topics.apply(lambda row: set_proportion(row, topic), axis=1).astype(float)

    df_topics.to_csv('Test.csv', index=False)

    return df_topics, df_terms_cpy, df_terms


def upload_results(df, aws_df, process_name, category):
    if '-idx-' in process_name:
        process_dir = process_name[:process_name.find('-idx-')]
    else:
        process_dir = process_name

        idx = process_dir.rfind('_')

        process_dir = replace_last(process_dir, process_dir[idx:], '')

    s3_path = f'{process_dir}/{process_dir}_{category}/df_{process_name}_{category}'

    response = aws_df.upload_to_s3(df, get_env_var('AWS_STORAGE_BUCKET', 'str'), s3_path)

    write_thread_logs(process_name, f"Results has been uploaded to s3 here: {response}")


def merge_process(output, process_name, aws_df):
    if output is None or output['output_uri'] is None:
        return

    df = aws_df.download_df_from_s3(output['input_uri'])

    if df is None:
        return

    df_topics, df_terms_cpy, df_terms = get_analysis_df(process_name, output, aws_df)

    if df_topics is None:
        return

    df = df.merge(df_topics, left_index=True, right_index=True)

    df.drop(['docname', 'lines', 'topic', 'proportion'], axis=1, inplace=True)

    dfs = {'analytics': df, 'all_terms': df_terms_cpy, 'terms_weight': df_terms}

    for category in dfs:
        upload_results(dfs[category], aws_df, process_name, category)
