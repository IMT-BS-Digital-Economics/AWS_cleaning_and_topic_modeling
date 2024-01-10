#!/usr/bin/env python3

"""
        AWS_S3_clean_emails

    Author: bricetoffolon
    Created on: 10/01/2024
    About: 

"""

from boto3 import client

from src.utils.env_handle import get_env_var

from json import dumps, loads


class Bedrock:
    def __create_client(self):
        self.aws = client(
            service_name="bedrock-runtime",
            region_name=get_env_var('AWS_REGION_NAME', 'str'),
            aws_access_key_id=get_env_var('AWS_ACCESS_KEY_ID', 'str'),
            aws_secret_access_key=get_env_var('AWS_SECRET_ACCESS_KEY', 'str')
        )

    def __init__(self):
        self.__create_client()

    def invoke(self, prompt, temperature, top_p):
        prompt_config = {
            "inputText": f'{prompt}',
            "textGenerationConfig": {
                "temperature": temperature,
                "topP": top_p
            }
        }

        response = self.aws.invoke_model(
            body=dumps(prompt_config),
            modelId="amazon.titan-text-express-v1"
        )

        response_body = loads(response.get("body").read())

        if 'results' in response_body and len(response_body.get('results')) > 0 and 'outputText' in \
                response_body.get('results')[0]:
            return response_body.get("results")[0].get('outputText')

        return None
