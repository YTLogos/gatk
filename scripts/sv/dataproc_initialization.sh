#!/bin/bash

set -eu

# This script initializes the master and worker nodes on a Google Dataproc
# Spark cluster to prepare them to run the GATK-SV pipeline.
#
# On the master node we install crcmod for faster gsutil transfers of large files
# and launch a hadoop distcp command to copy the reference and test data sets down
# from a bucket to the hdfs file system on the cluster.
#
# On the worker nodes we install SGA and its dependencies.

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

if [[ "${ROLE}" == 'Master' ]]; then

    # TODO: find way to paramaterize the bucket name
	hadoop distcp gs://sv-data-dsde-dev/* hdfs://`/usr/share/google/get_metadata_value hostname`:8020/

else 
	
	PACKAGES="zlib1g-dev libsparsehash-dev wget make automake g++-4.9 cmake"

	BAM_TAR=https://github.com/pezmaster31/bamtools/archive/v2.3.0.tar.gz
	BAM_DIR=/tmp/bam

	SGA_TAR=https://github.com/jts/sga/archive/v0.10.14.tar.gz
	SGA_DIR=/tmp/sga

    n=0
    until [ $n -ge 5 ]
    do
	    echo "deb http://http.us.debian.org/debian testing main" > /etc/apt/sources.list
	    apt-get update -y && \
    	    DEBIAN_FRONTEND=noninteractive apt-get install -yq --no-install-recommends ${PACKAGES} && break
    	n=$[$n+1]
        sleep 15
   done

	mkdir -p ${BAM_DIR}
	cd ${BAM_DIR} && \
    	wget ${BAM_TAR} --no-check-certificate --output-document - \
		| tar xzf - --directory . --strip-components=1
	cd ${BAM_DIR} && \
    	mkdir -p build && \
    	cd build && \
    	cmake .. && \
    	make

	mkdir -p ${SGA_DIR}
	cd ${SGA_DIR} && \
    	wget ${SGA_TAR} --no-check-certificate --output-document - \
		| tar xzf - --directory . --strip-components=2 && \
    	./autogen.sh && \
    	./configure --with-bamtools=${BAM_DIR} && \
    	make && \
    	make install && \
        rm -rf ${SGA_DIR}
                
fi
