#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 11/10/2023
    About: To handle arguments of algorithm

"""

from argparse import ArgumentParser


def create_args():
    parser = ArgumentParser(
        description="This algorithm is responsible to use AWS Python SDK to manage different service such as AWS S3 "
                    "and AWS Comprehend. To use it, You must specify the mode you want to use and a bucket URI or a "
                    "file URI depending on what you want to do ! Enjoy :)"
    )

    parser.add_argument("name", type=str, help="A name for your process")

    parser.add_argument("mode", type=str, action="store",
                        choices={'cleaning', 'topic_analysis', 'merging', 'all'},
                        help="Cleaning: The email in the given db will be just cleaned and send as parquet on a S3 "
                             "bucket or Topic Analysis: The email will be cleaned and then analyzed using topic "
                             "modeling or Merging: If you only want to merge an already existing cleaned file with "
                             "the results from AWS comprehend you can go for this: You must provide a JobId of the AWS "
                             "Comprehend Job and a fileUri of the cleaned parquet file that was used to launch the job "
                             "or All: It will run through each steps")

    parser.add_argument('--bucketUri', type=str, dest="bucketUri", action="store",
                        help="Provide the bucket URI if you want to treat with a whole "
                             "bucket content")

    parser.add_argument('--fileUri', type=str, dest="fileUri", action="store",
                        help="Provide the file URI if you want to treat only one document")

    parser.add_argument('--job-id', type=str, dest="jobId", action="store", help="Only in merging mode you need to "
                                                                                  "provide the JobId of the AWS "
                                                                                  "Comprehend Job to start merging "
                                                                                  "from it")

    parser.add_argument('--s', dest="treatSubject", action="store_true", help="Add this argument if you want to treat "
                                                                            "the subject of each emails too")

    return parser.parse_args()


def handle_args():
    args = vars(create_args())

    if args.get('mode') != "merging" and args.get('bucketUri') is None and args.get('fileUri') is None:
        raise Exception('You should specify a bucket uri or a file uri\nFor more: see main.py -h')

    if args.get('mode') == "merging" and (args.get('job_id') is None or args.get('file_uri') is None):
        raise Exception('You should add a Job Id referring to the AWS Comprehend Job you want to merge results and a '
                        'file_uri referring to the cleaned file from\nFor more: see main.py -h')

    return args
