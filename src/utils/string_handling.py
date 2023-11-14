#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 13/11/2023
    About: 

"""


def replace_last(s, old, new):
    if old not in s:
        return None

    return new.join(s.rsplit(old, 1))
