import requests
import json

# Base url for Italian Ministry of University data. This is always the same.
base_url = 'http://dati.ustat.miur.it/api/3/action/datastore_search?resource_id='

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
    'spesa_atenei_2021': '93493e55-63d8-4c08-a6b7-87ad3c781a22'
}

for key in resources_id:

    # Construct the url for the dataset of interest
    resource_information_url = base_url + resources_id[key]

    # Make the HTTP request
    resource_information = requests.get(resource_information_url)

    # Use the json module to load CKAN's response into a dictionary
    resource_dict = json.loads(resource_information.content)

    with open(f"{key}.json", "w") as outfile:
        json.dump(resource_dict['result']['records'], outfile)
