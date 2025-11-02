import os
from urllib.parse import urlparse

SPARK_MASTER_HOST = os.getenv("SPARK_MASTER_HOST", "local[*]")
DATASET_PATH = os.getenv("DATASET_PATH", "./train.csv")
DRIVER_MEMORY = os.getenv("DRIVER_MEMORY", "16g")

HTTP_PROXY = os.getenv("http_proxy")
HTTPS_PROXY = os.getenv("https_proxy")

def parse_proxy(proxy_url):
    if not proxy_url:
        return None, None
    parsed = urlparse(proxy_url)
    return parsed.hostname, parsed.port

HTTP_PROXY_HOST, HTTP_PROXY_PORT = parse_proxy(HTTP_PROXY)
HTTPS_PROXY_HOST, HTTPS_PROXY_PORT = parse_proxy(HTTPS_PROXY)
  
