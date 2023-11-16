#!/bin/bash

BUCKET="dse-infra-dev-us-west-2-mwaa"
ENVIRONMENT="dse-infra-dev-us-west-2-mwaa-environment"

# Copy the dags
aws s3 sync dags/$SUBDIR s3://${BUCKET}/dags/pems/ --delete --exclude "*__pycache__*"
