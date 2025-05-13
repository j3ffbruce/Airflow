import requests
from requests import Response
import json

def extract_api(config: dict) -> json:
   data: Response = requests.get(
       url=config["endpoints"]["randomuser"]["url"],
       timeout=config["endpoints"]["randomuser"]["timeout"]
   )
   data: json = json.loads(data.text)
   return data