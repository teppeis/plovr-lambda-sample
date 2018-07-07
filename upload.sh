#!/bin/bash

jar=$1
filename=$(basename "$jar")
lambda=$2
s3bucket=com.teppeis.sample.hello-lambda
s3path=s3://"$s3bucket"/"$filename"

aws s3 cp "$jar" "$s3path"
aws lambda update-function-code \
    --region ap-northeast-1 \
    --function-name "$lambda" \
    --s3-bucket "$s3bucket" \
    --s3-key "$filename" \
    --publish
