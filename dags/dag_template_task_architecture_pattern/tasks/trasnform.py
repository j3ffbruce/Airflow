import json

def transform(data: json) -> str:
    return  data["results"][0]["email"]