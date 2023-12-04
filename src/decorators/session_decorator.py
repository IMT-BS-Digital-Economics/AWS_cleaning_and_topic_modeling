#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 30/09/2023
    About: A decorator that verify the session validity in time and renew the session if neccessary

"""

from time import time

from src.utils.env_handle import get_env_var


def verify_session(renew_session):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            session_expiration = get_env_var("AWS_SESSION_EXPIRATION_IN_S", 'int')

            if not time() - self.session_time > session_expiration:
                return func(self, *args, **kwargs)

            renew_session(self)

            self.session_time = time()

            return func(self, *args, **kwargs)

        return wrapper

    return decorator
