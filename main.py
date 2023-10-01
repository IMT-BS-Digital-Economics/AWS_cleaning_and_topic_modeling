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

    parser.add_argument("--mode", dest="mode", action="store", choices={'cleaning', 'topic_analysis'}, required=True, help="-Cleaning: The email in the given db will be just cleaned and send as parquet on a S3 bucket\n-Topic Analysis: The email will be cleaned and then analyzed using topic modeling")

    parser.add_argument("database", type=str, help='a string: specify the database to use to access data based on AWS '
                                                   'Athena')
    parser.add_argument("table", type=str, help='a string: specify the table to use to access data based on AWS Athena')

    return parser.parse_args()


def main():
    args = handle_args()

    aws_df = AwsDf()

    if args.mode == "topic analysis":
        aws_comprehend = AwsComprehend()
    else:
        aws_comprehend = None

    loop(args.database, args.table, aws_df, aws_comprehend, args.mode)


if __name__ == "__main__":
    main()
