#!/usr/bin/env python3

"""
        training

    Author: bricetoffolon
    Created on: 25/09/2023
    About: To retrieve a .env variable

"""

from ast import literal_eval

from os import environ

from dotenv import dotenv_values


def get_env() -> dict:
    env: dict = dotenv_values('.env')

    if not env:
        env: dict = environ

    return env


def get_env_var(var_name: str, var_type: str) -> any:
    env: dict = get_env()

    if var_name not in env:
        raise Exception(f'{var_name} is not defined in your environment currently')

    env_var = env[var_name]

    try:
        env_var = literal_eval(env_var)
    except Exception as e:
        return env_var

    if var_type not in str(type(env_var)):
        raise Exception(f'{var_name} is {type(env_var)} but expected {var_type}')

    return env_var