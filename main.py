#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 25/09/2023
    About: To start operations

"""

from src.aws.comprehend import AwsComprehend
from src.aws.python_sdk import AwsDf

from src.loop import loop
from src.thread import thread_process

from src.utils.args import handle_args


def main():
    args = handle_args()

    aws_df = AwsDf()

    aws_comprehend = None

    if args.mode == "topic_analysis" or args.mode == "all" or args.mode == "merging":
        aws_comprehend = AwsComprehend()

    if args.bucket_uri is not None:
        loop(aws_df, aws_comprehend, args.mode, args['bucket_uri'])

    if args.file_uri is not None:
        thread_process(aws_df, aws_comprehend, args.file_uri, args.mode, job_id=args.job_id if args.mode == "merging" else None)


if __name__ == "__main__":
    main()
