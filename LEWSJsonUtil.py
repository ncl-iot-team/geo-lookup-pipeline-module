import json

class JsonDataUtil():

    def __init__(self,json_string):

        self.json_data = json_string

        self.new_json_data = {}

        if 'lews_metadata' not in self.json_data:

            self.new_json_data['raw_data'] = self.json_data
            self.new_json_data['lews_metadata'] = {}

    def get_value(self,field):
        return self.new_json_data['raw_data'][field]
      #  print(self.json_data)

    def add_metadata(self,key,value):
        self.new_json_data['lews_metadata'][key]=value

        #print(self.json_data)

    def get_json(self):
        return self.new_json_data


    
# json_util = JsonDataUtil('{"raw_data": { "text": "Success", "text2": "New Success"}, "lews_metadata": { "new":"1" } }')

# print(json_util.get_value("text"))
# print(json_util.get_value("text2"))

# json_util = JsonDataUtil('{ "text": "Success2", "text2": "New Success2"}')

# print(json_util.get_value("text"))
# print(json_util.get_value("text2"))


# json_util.add_metadata('Newkey','Newvalue')

# json_util.add_metadata('Lat','1.0324')

# json_util.add_metadata('Long','-2423')

