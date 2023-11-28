#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 30/09/2023
    About: A class to work with AWS pandas SDK (awsWrangler)

"""

from time import time, sleep

from pyarrow.lib import ArrowInvalid

from boto3 import Session, client

import awswrangler as wr

from src.utils.env_handle import get_env_var

from src.decorators.session_decorator import verify_session


class AwsDf:
    def __create_session(self):
        return Session(
            region_name=get_env_var('AWS_REGION_NAME', 'str'),
            aws_access_key_id=get_env_var('AWS_ACCESS_KEY_ID', 'str'),
            aws_secret_access_key=get_env_var('AWS_SECRET_ACCESS_KEY', 'str')
        )

    def __init__(self):
        self.session_time = time()

        self.aws_session = self.__create_session()

    @verify_session(renew_session=__create_session)
    def download_df_from_s3(self, file_uri):
        if not wr.s3.does_object_exist(file_uri, boto3_session=self.aws_session):
            raise Exception(f'No bucket found with this path: {file_uri}')

        try:
            return wr.s3.read_parquet(path=file_uri, boto3_session=self.aws_session)
        except ArrowInvalid:
            return wr.s3.read_csv(path=file_uri, boto3_session=self.aws_session)

    def get_s3_bucket_obj_list(self, bucket_link):
        return wr.s3.list_objects(bucket_link, boto3_session=self.aws_session)

    def cpy_object(self, obj_path, src_path, new_path):
        return wr.s3.copy_objects(
            paths=[obj_path],
            source_path=src_path,
            target_path=new_path,
            boto3_session=self.aws_session
        )

    @verify_session(renew_session=__create_session)
    def get_df_from_athena(self, query, db):
        return wr.athena.read_sql_query(query, db, boto3_session=self.aws_session, ctas_approach=False)

    @verify_session(renew_session=__create_session)
    def upload_to_s3(self, df, bucket, name, ext="parquet"):
        bucket_path = f"s3://{bucket}/{name}.{ext}"

        response = {}

        while 'paths' not in response or len(response['paths']) == 0:
            if response != {}:
                sleep(2 * 60)

            if ext == "csv":
                response = wr.s3.to_csv(df, index=False, path=bucket_path, boto3_session=self.aws_session)
            else:
                response = wr.s3.to_parquet(df, index=False, path=bucket_path, boto3_session=self.aws_session)

        return response

    def download_file_using_client(self, bucket, file_name, output_path):
        aws_client = client(
            's3',
            region_name=get_env_var('AWS_REGION_NAME', 'str'),
            aws_access_key_id=get_env_var('AWS_ACCESS_KEY_ID', 'str'),
            aws_secret_access_key=get_env_var('AWS_SECRET_ACCESS_KEY', 'str')
        )

        aws_client.download_file(bucket, file_name, output_path)
