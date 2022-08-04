import requests
import wget
import io
import os
import json
from google.cloud import storage

# Base url for Italian Ministry of University data. This is always the same.
base_url = 'http://dati.ustat.miur.it/api/3/action/datastore_search?limit=32000&resource_id='

# Specify the datasets we are interested in:
resources_id = {
    'atenei': 'a332a119-6c4b-44f5-80eb-3aca45a9e8e8',
    'imm_atenei': '14938cd2-7683-4fff-b5e0-0741402a0dd2',
    'lau_atenei': '88acd482-9d75-44d1-ab16-aa04524f5d94',
    'isc_atenei': '32d26e28-a0b5-45f3-9152-6072164f3e63',
    'imm_forstd': '81610f0b-f1c2-4e45-b7a5-d03473ad4bf8',
    'lau_forstd': 'bf77a087-34fe-4805-bcf4-b5d61cbfc1f3',
    'isc_forstd': '27ba8d3b-9a54-4a75-8798-2010bc7c205a',
    'interventi_atenei_2021': '426eb254-de5b-4b11-aeef-97127e746783',
    'interventi_atenei_2020': '252d2c22-beda-40de-be1a-cc316d3dbd38',
    'interventi_atenei_2019': 'cd857078-acd9-44ad-879d-29ecd8810e7b',
    'interventi_atenei_2018': 'dea84a7d-1b9e-402c-82af-09ddd855760a',
    'interventi_atenei_2017': 'a3ade394-0f81-4f19-8704-6618cc719528',
    'interventi_atenei_2016': '8185e6c8-eceb-4f9f-8275-17dacc848b00',
}

json_obj = json.dumps(resources_id)
f = open("resources_id.json", "w")
f.write(json_obj)

# Access the client with keys
storage_client = storage.Client.from_service_account_json(
    "INSERT JSON FILE WITH CLOUD STORAGE KEYS")

# Define the bucket name
bucket_name = storage_client.get_bucket("INSERT BACKET NAME")

# Download the data in the local machine


def download_data(base_url, resources_id, key):
    # Construct the url for the dataset of interest
    resource_information_url = base_url + resources_id[key]
    # Make the HTTP request
    resource_information = requests.get(resource_information_url)
    # Use the json module to load CKAN's response into a dictionary
    resource_dict = json.loads(resource_information.content)
    with open(f"{key}.json", "w") as outfile:
        json.dump(resource_dict['result']['records'], outfile)


# Define a function to upload files to the bucket
def upload_blob(bucket_name, destination_blob_name):
    filename = f"{key}.json"
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(filename, content_type='text/json')
    os.remove(filename)


# Load the data into the bucket
for key in resources_id:
    download_data(base_url, resources_id, key)
    destination_blob_name = (f"{key}.json")
    upload_blob(bucket_name, destination_blob_name)
