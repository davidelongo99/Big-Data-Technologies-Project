from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc
import re
import argparse

# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0
#    http://www.apache.org/licenses/LICENSE-2.0

"""
This quickstart sample walks a user through creating a Cloud Dataproc
cluster, submitting a PySpark job from Google Cloud Storage to the
cluster, reading the output of the job and deleting the cluster, all
using the Python client library.
Args:
    project_id (string): Project to use for creating resources.
    region (string): Region where the resources should live.
    cluster_name (string): Name to use for creating a cluster.
    job_file_path (string): Path of the PySpark job file in GCS. 

"""

def quickstart(project_id, region, cluster_name, job_file_path):
    # Create the cluster client.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id,
                 "region": region, "cluster": cluster}
    )
    result = operation.result()

    print("Cluster created successfully: {}".format(result.cluster_name))

    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": job_file_path},
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(f"Job finished successfully: {output}")

    # Delete the cluster once the job has terminated.
    operation = cluster_client.delete_cluster(
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    operation.result()

    print("Cluster {} successfully deleted.".format(cluster_name))


project_id = "INSERT PROJECT ID"
region = "europe-west1"
cluster_name = "py-cc-test-{}".format(str(uuid.uuid4()))
job_file_path = "INSERT JOB FILE PATH"

quickstart(project_id, region, cluster_name, job_file_path)
