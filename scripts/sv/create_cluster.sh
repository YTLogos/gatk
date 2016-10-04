#!/bin/bash

# This script creates a Google Dataproc cluster suitable for running the GATK-SV pipeline.

set -eu

PROJECT=$1
CLUSTER_NAME=$2
INITIALIZATION_BUCKET=$3

gcloud dataproc clusters create ${CLUSTER_NAME} \
    --zone us-central1-a \
    --master-machine-type n1-highmem-8 \
    --num-worker-local-ssds 1 \
    --worker-machine-type n1-highmem-16 \
    --master-boot-disk-size 500 \
    --worker-boot-disk-size 500 \
    --num-workers 10 \
    --image-version 1.0 \
    --project ${PROJECT} \
    --initialization-actions gs://${INITIALIZATION_BUCKET}/initialization_scripts/dataproc_initialization.sh \
    --initialization-action-timeout 60m

