#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 25/09/2023
    About: To start operations

"""

from argparse import ArgumentParser

from src.aws.comprehend import AwsComprehend
from src.aws.python_sdk import AwsDf

from src.loop import loop


def handle_args():
    parser = ArgumentParser(
        description="Select which Database and Table for your topic modeling process"
    )

    parser.add_argument("--mode", type=str, dest="mode", action="store", choices={'cleaning', 'topic_analysis'}, required=True, help="-Cleaning: The email in the given db will be just cleaned and send as parquet on a S3 bucket\n-Topic Analysis: The email will be cleaned and then analyzed using topic modeling")

    parser.add_argument("bucket_uri", type=str, help="Provide the bucket URI that contains each parquet files you "
                                                     "want to clean and use for topic analysis")

    return parser.parse_args()


def main():
    args = handle_args()

    aws_df = AwsDf()

    if args.mode == "topic analysis":
        aws_comprehend = AwsComprehend()
    else:
        aws_comprehend = None

    loop(aws_df, aws_comprehend, args.mode, args.bucket_uri)


if __name__ == "__main__":
    main()
