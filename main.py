#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 25/09/2023
    About: To start operations

"""

from src.aws.comprehend import AwsComprehend
from src.aws.python_sdk import AwsDf
from src.launch_analysis_process import launch

from src.utils.args import handle_args

def main():
    args = handle_args()

    settings = {
        'name': args.get('name'), 'mode': args.get('mode'), 'performOnSubject': args.get('treatSubject'), 'awsDf': AwsDf()
    }

    if settings.get('mode') in ["topic_analysis", "all", "merging"]:
        settings['awsComprehend'] = AwsComprehend()

    launch(settings, args)

if __name__ == "__main__":
    main()
