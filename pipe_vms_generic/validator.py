from jsonschema import validate

import json

def validateJson(data):
    with open("./assets/vms_list_schema.json") as vms_schema:
        validate(instance=data, schema=json.loads(vms_schema.read()))
