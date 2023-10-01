#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 30/09/2023
    About: A class to work with AWS pandas SDK (awsWrangler)

"""

from time import time

from boto3 import Session

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

        self.aws = self.__create_session()

    @verify_session(renew_session=__create_session)
    def get_bucket_as_df(self, bucket_link):
        if not wr.s3.does_object_exist(bucket_link, boto3_session=self.aws):
            raise Exception(f'No bucket found with this path: {bucket_link}')

        return wr.s3.read_parquet(path=bucket_link, boto3_session=self.aws)

    @verify_session(renew_session=__create_session)
    def get_df_from_athena(self, query, table):
        return wr.athena.read_sql_query(query, table, boto3_session=self.aws)

    @verify_session(renew_session=__create_session)
    def upload_to_s3(self, df, bucket, name, ext="csv"):
        bucket_path = f"s3://{bucket}/{name}.{ext}"

        if type == "csv":
            return wr.s3.to_csv(df, index=False, path=bucket_path, boto3_session=self.aws)
        else:
            return wr.s3.to_parquet(df, index=False, path=bucket_path, boto3_session=self.aws)


