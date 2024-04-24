import os
import requests

base_url = "https://datasets-server.huggingface.co"

def api_request(api_url):
    headers = {"Authorization": f"Bearer {os.environ['HUGGINGFACE_API_TOKEN']}"}
    response = requests.get(api_url, headers=headers)
    return response.json()

def get_splits(ds_name='HuggingFaceFW/fineweb'):
    api_url = f"https://datasets-server.huggingface.co/splits?dataset={ds_name}"
    return api_request(api_url)

def get_stats(dump_name, split='train', ds_name='HuggingFaceFW/fineweb'):
    api_url = f"{base_url}/statistics?dataset={ds_name}&config={dump_name}&split={split}"
    return api_request(api_url)

def get_size_stats(ds_name='HuggingFaceFW/fineweb'):
    api_url = f"{base_url}/size?dataset={ds_name}"
    return api_request(api_url)

def search(dump_name, query, offset=0, length=10, split='train', ds_name='HuggingFaceFW/fineweb'):
    api_url = f"{base_url}/search?dataset={ds_name}&config={dump_name}&split={split}&query={query}&offset={offset}&length={length}"
    return api_request(api_url)