# AWS_cleaning_and_topic_modeling

## About the script

This script is designed to clean and run topic modeling over emails bodies

## Requirements
 
* Require __Python3.8__, you can download it there : https://www.python.org/downloads
* You need to install required packages with the following command :  `pip3 install -r requirements.txt`

## Configuration

You need to create a .env with this syntax

```

# AWS Credentials

AWS_REGION_NAME=(Provide the region name to use to connect to your AWS account)

AWS_ACCESS_KEY_ID=(You need to create your own ACCESS_KEY to get this information on AWS in security credentials)
AWS_SECRET_ACCESS_KEY=(You need to create your own ACCESS_KEY to get this information on AWS in security credentials)

AWS_IAM_USER=arn:aws:iam::{YOUR_USER_ID} (Go to the AWS IAM Panel to found this information)
AWS_IAM_ROLE=role/{YOUR_ROLE} (Go to the AWS IAM Panel in the Role section to found this information)

AWS_SESSION_EXPIRATION_IN_S=3000 (Equivalent to 50 minutes)

# S3 Settings

AWS_TMP_BUCKET=(To store cleaned data for topic analysis)
AWS_RESULT_BUCKET=(To store result of topic analysis)
AWS_STORAGE_BUCKET=(To store cleaned data when only cleaning)

# Comprehend Settings

NUMBER_OF_TOPIC=(Between 10 & 100 according to AWS Topic Modeling documentation)

# Thread

MAX_THREAD=(Number of threads to be used by the algo, depend of the machine capacity)

# CSV INPUT

BODY_VAR = Name of the column that contain the body of the emaik inside CSV files used as input

```

## Start the project

To start the project:  ``` python main.py --h```

To get the usage of the project and run it for the first time !

**Enjoy!**

