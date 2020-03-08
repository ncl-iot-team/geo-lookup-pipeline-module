import json

class JsonDataUtil():

    def __init__(self,value_string):

        self.json_data = json.loads(value_string)

        self.new_json_data = {}

        if 'lews_metadata' not in self.json_data:

            self.new_json_data['raw_data'] = self.json_data
            self.new_json_data['lews_metadata'] = {}

    def get_value(self,field):
        return self.new_json_data['raw_data'][field]

    def add_metadata(self,key,value):
        self.new_json_data['lews_metadata'][key]=json.loads(value)


    def get_json(self):
        return json.dumps(self.new_json_data)


    

