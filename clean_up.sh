#!/bin/bash

# SPDX-FileCopyrightText: Copyright 2021 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: MIT-0

# export EKSCLUSTER_NAME=eks-quickstart
# export AWS_REGION=us-east-1

export ACCOUNTID=$(aws sts get-caller-identity --query Account --output text)
export EMRCLUSTER_NAME=emr-on-$EKSCLUSTER_NAME
export S3TEST_BUCKET=${EMRCLUSTER_NAME}-${ACCOUNTID}-${AWS_REGION}

echo "delete EMR on EKS IAM execution role "
export ROLE_NAME=$EMRCLUSTER_NAME-execution-role
export POLICY_ARN=arn:aws:iam::$ACCOUNTID:policy/$ROLE_NAME-policy
aws iam detach-role-policy --role-name $ROLE_NAME --policy-arn $POLICY_ARN
aws iam delete-role --role-name $ROLE_NAME
aws iam delete-policy --policy-arn $POLICY_ARN

echo "delete S3 bucket $S3TEST_BUCKET"
aws s3 rm s3://$S3TEST_BUCKET --recursive
aws s3api delete-bucket --bucket $S3TEST_BUCKET

echo "delete EKS cluster"
eksctl delete cluster --name $EKSCLUSTER_NAME

echo "delete EMR virtual cluster"
export VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters --query "virtualClusters[?name == '${EMRCLUSTER_NAME}' && state == 'RUNNING'].id" --output text)
aws emr-containers delete-virtual-cluster --id $VIRTUAL_CLUSTER_ID

echo "delete Glue catalog"
tablelist=$(aws glue get-tables --database-name default --query 'TableList[?starts_with(StorageDescriptor.Location, `s3://emr-on-eks-quickstart-`) == `true`].Name' --output text)
for name in $tablelist
do
	echo "delete table $name"
	aws glue delete-table --database-name default --name $name
done
