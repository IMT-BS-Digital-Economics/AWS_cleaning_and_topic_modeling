#!/bin/bash

source ./.env.git

echo "Cloning repository with GIT_TOKEN: $GIT_TOKEN"

git clone https://"$GIT_NAME":"$GIT_TOKEN"@github.com/"$GIT_PROJECT".git