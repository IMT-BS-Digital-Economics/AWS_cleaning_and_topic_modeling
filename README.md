# AWS_cleaning_and_topic_modeling

<p align="center">
  <img src="https://th.bing.com/th/id/OIG.HG4RearsJ_yzo5dTMKPX?pid=ImgGn" alt="Project Logo" height="auto" width="300" style="border-radius:50%">
</p>

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

### Arguments

```
  --name NAME           A name for your process
  --mode {cleaning,merging,all,topic_analysis}
                        - Cleaning: The email in the given db will be just
                        cleaned and send as parquet on a S3 bucket
                        - Topic Analysis: The email will be cleaned and then analyzed
                        using topic modeling 
                        - Merging: If you only want to merge an already existing cleaned file with the
                        results from AWS comprehend you can go for this: You
                        must provide a JobId of the AWS Comprehend Job and a
                        fileUri of the cleaned parquet file that was used to
                        launch the job
                        - All: It will run through each steps
  --bucket-uri BUCKET_URI
                        Provide the bucket URI if you want to treat with a
                        whole bucket content
  --file-uri FILE_URI   Provide the file URI if you want to treat only one
                        document
  --job-id JOB_ID       Only in merging mode you need to provide the JobId of
                        the AWS Comprehend Job to start merging from it
```

## Start the project using Docker

Make sure the latest version of docker is installed on your machine. Then run the following command:

```docker build -t aws-clean-topic-modeling .```

Then run this:

```docker run -d -v "$(pwd)":/app/ aws-clean-topic-modeling [arguments]```

Enjoy :)

