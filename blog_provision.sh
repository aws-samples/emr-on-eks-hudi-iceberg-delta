#!/bin/bash

# // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# // SPDX-License-Identifier: MIT-0

# Define params
# export EKSCLUSTER_NAME=eks-quickstart
# export AWS_REGION=us-east-1
export EMR_NAMESPACE=emr
export EKS_VERSION=1.21
export EMRCLUSTER_NAME=emr-on-$EKSCLUSTER_NAME
export ROLE_NAME=${EMRCLUSTER_NAME}-execution-role
export ACCOUNTID=$(aws sts get-caller-identity --query Account --output text)
export S3TEST_BUCKET=${EMRCLUSTER_NAME}-${ACCOUNTID}-${AWS_REGION}

echo "==============================================="
echo "  setup IAM roles ......"
echo "==============================================="

# create S3 bucket for application
if [ $AWS_REGION=="us-east-1" ]; then
  aws s3api create-bucket --bucket $S3TEST_BUCKET --region $AWS_REGION 
else
  aws s3api create-bucket --bucket $S3TEST_BUCKET --region $AWS_REGION --create-bucket-configuration LocationConstraint=$AWS_REGION
fi
# Create a job execution role (https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/creating-job-execution-role.html)
cat >/tmp/job-execution-policy.json <<EOL
{
    "Version": "2012-10-17",
    "Statement": [ 
        {
            "Effect": "Allow",
            "Action": ["s3:PutObject","s3:DeleteObject","s3:GetObject","s3:ListBucket"],
            "Resource": [
              "arn:aws:s3:::${S3TEST_BUCKET}",
              "arn:aws:s3:::${S3TEST_BUCKET}/*"
            ]
        }, 
        {
            "Effect": "Allow",
            "Action": [ "logs:PutLogEvents", "logs:CreateLogStream", "logs:DescribeLogGroups", "logs:DescribeLogStreams", "logs:CreateLogGroup" ],
            "Resource": [ "arn:aws:logs:*:*:*" ]
        },
        {
          "Effect": "Allow",
          "Action": ["glue:Get*","glue:BatchCreatePartition","glue:UpdateTable","glue:CreateTable"],
          "Resource": [
              "arn:aws:glue:${AWS_REGION}:${ACCOUNTID}:catalog",
              "arn:aws:glue:${AWS_REGION}:${ACCOUNTID}:database/*",
              "arn:aws:glue:${AWS_REGION}:${ACCOUNTID}:table/*"
            ]
        },
        {
          "Effect": "Allow",
          "Action": ["glue:Get*","glue:BatchCreatePartition","glue:UpdateTable","glue:CreateTable"],
          "Resource": [
              "arn:aws:glue:${AWS_REGION}:${ACCOUNTID}:catalog",
              "arn:aws:glue:${AWS_REGION}:${ACCOUNTID}:database/*",
              "arn:aws:glue:${AWS_REGION}:${ACCOUNTID}:table/*"
            ]
        },
        {
            "Sid": "DynamoDBLockManager",
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeTable",
                "dynamodb:CreateTable",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:GetItem",
                "dynamodb:BatchGetItem"
            ],
            "Resource": [
                "arn:aws:dynamodb:${AWS_REGION}:${ACCOUNTID}:table/myIcebergLockTable",
                "arn:aws:dynamodb:${AWS_REGION}:${ACCOUNTID}:table/myIcebergLockTable/index/*",
                "arn:aws:dynamodb:${AWS_REGION}:${ACCOUNTID}:table/myHudiLockTable"
            ]
        }
    ]
}
EOL

cat >/tmp/trust-policy.json <<EOL
{
  "Version": "2012-10-17",
  "Statement": [ {
      "Effect": "Allow",
      "Principal": { "Service": "eks.amazonaws.com" },
      "Action": "sts:AssumeRole"
    } ]
}
EOL

aws iam create-policy --policy-name $ROLE_NAME-policy --policy-document file:///tmp/job-execution-policy.json
aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document file:///tmp/trust-policy.json
aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn arn:aws:iam::$ACCOUNTID:policy/$ROLE_NAME-policy

echo "==============================================="
echo "  Create EKS Cluster ......"
echo "==============================================="

cat <<EOF >/tmp/ekscluster.yaml
---
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: $EKSCLUSTER_NAME
  region: $AWS_REGION
  version: "$EKS_VERSION"
vpc:
  clusterEndpoints:
      publicAccess: true
      privateAccess: true  
availabilityZones: ["${AWS_REGION}a","${AWS_REGION}b"]   
iam:
  withOIDC: true
  serviceAccounts:
  - metadata:
      name: cluster-autoscaler
      namespace: kube-system
      labels: {aws-usage: "cluster-ops"}
    wellKnownPolicies:
      autoScaler: true
    roleName: eksctl-cluster-autoscaler-role
managedNodeGroups: 
  - name: mn-od
    availabilityZones: ["${AWS_REGION}b"] 
    instanceType: c5.4xlarge
    volumeSize: 200
    volumeType: gp3
    minSize: 1
    desiredCapacity: 1
    maxSize: 10
    tags:
      # required for cluster-autoscaler auto-discovery
      k8s.io/cluster-autoscaler/enabled: "true"
      k8s.io/cluster-autoscaler/$EKSCLUSTER_NAME: "owned"  

# enable all of the control plane logs
cloudWatch:
 clusterLogging:
   enableTypes: ["*"]
EOF

# create eks cluster in two AZs
eksctl create cluster -f /tmp/ekscluster.yaml
# if EKS cluster exists, comment out the line above, uncomment this line
# eksctl create nodegroup -f /tmp/ekscluster.yaml
aws eks update-kubeconfig --name $EKSCLUSTER_NAME --region $AWS_REGION

echo "==============================================="
echo "  Enable EMR on EKS ......"
echo "==============================================="

# Create kubernetes namespace for EMR on EKS
kubectl create namespace $EMR_NAMESPACE

# Enable cluster access for Amazon EMR on EKS in the 'emr' namespace
eksctl create iamidentitymapping --cluster $EKSCLUSTER_NAME --namespace $EMR_NAMESPACE --service-name "emr-containers"
aws emr-containers update-role-trust-policy --cluster-name $EKSCLUSTER_NAME --namespace $EMR_NAMESPACE --role-name $ROLE_NAME

# Create emr virtual cluster
aws emr-containers create-virtual-cluster --name $EMRCLUSTER_NAME \
  --container-provider '{
        "id": "'$EKSCLUSTER_NAME'",
        "type": "EKS",
        "info": { "eksInfo": { "namespace": "'$EMR_NAMESPACE'" } }
    }'

echo "==============================================="
echo "  Configure EKS Cluster ......"
echo "==============================================="
# Install k8s metrics server
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

# Install Cluster Autoscaler that automatically adjusts the number of nodes in EKS
cat <<EOF >/tmp/autoscaler-config.yaml
---
autoDiscovery:
    clusterName: $EKSCLUSTER_NAME
awsRegion: $AWS_REGION
image:
    tag: v1.21.1
podAnnotations:
    cluster-autoscaler.kubernetes.io/safe-to-evict: 'false'
extraArgs:
    skip-nodes-with-system-pods: false
    scale-down-unneeded-time: 1m
    scale-down-unready-time: 2m
rbac:
    serviceAccount:
        create: false
        name: cluster-autoscaler
EOF

helm repo add autoscaler https://kubernetes.github.io/autoscaler
helm install nodescaler autoscaler/cluster-autoscaler --namespace kube-system --values /tmp/autoscaler-config.yaml --debug

# echo "==================================================================="
# echo "  Patch k8s user permission for PVC ......"
# echo "==================================================================="
curl -o rbac_pactch.py https://raw.githubusercontent.com/aws/aws-emr-containers-best-practices/main/tools/pvc-permission/rbac_patch.py
python3 rbac_pactch.py -n $EMR_NAMESPACE -p


echo "Finished, proceed to submitting a job"
