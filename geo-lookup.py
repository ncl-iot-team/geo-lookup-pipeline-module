from abc import ABC, abstractmethod
from kafka import KafkaConsumer, KafkaProducer
import json
import os
import requests
import traceback
import spacy
import LEWSJsonUtil as util

#--------------- Template Code, Avoid changing anything in this section --------------------------# 
class AbstractKafkaInStreamProcessor(ABC):
        
    def produce_data_kafka(self,record) -> None:
      
      self.producer.send(topic=self.target_topic,value=record)
      #print("Processed Record Sent")



    @abstractmethod
    def process_data(self,record) -> None:
        
        return record



    def kafka_in_stream_processor(self) -> None:

        for message in self.consumer:
        
            self.processed_record = self.process_data(message)
        
            self.produce_data_kafka(self.processed_record)




    def __init__(self,processor_name,source_topic,target_topic):

        self.processor_name = processor_name
        
        self.source_topic = source_topic
        
        self.target_topic = target_topic
        
        self.bootstrap_servers = os.getenv('KAFKA_BROKER','host.docker.internal:9092')
        #self.bootstrap_servers = 'localhost:9092'
        
        print("Initializing Kafka In-Stream Processor Module")
        
        self.consumer = KafkaConsumer(source_topic,group_id = self.processor_name,
                                      bootstrap_servers = self.bootstrap_servers,
                                      value_deserializer=lambda m: json.loads(m.decode('utf-8')))

       # self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)

        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      value_serializer = lambda v: json.dumps(v).encode('utf-8'))




def run(abstract_class: AbstractKafkaInStreamProcessor) -> None:
    """
    The client code calls the template method to execute the algorithm. Client
    code does not have to know the concrete class of an object it works with, as
    long as it works with objects through the interface of their base class.
    """

    # ...
    abstract_class.kafka_in_stream_processor()
    # ...


#-------------------------Template Code Ends Here ------------------------------------#

class ConKafkaInStreamProcessor(AbstractKafkaInStreamProcessor):
    
    def __init__(self,processor_name,source_topic,target_topic):
        super(ConKafkaInStreamProcessor,self).__init__(processor_name,source_topic,target_topic)
        self.output_fields = 'text'
        self.gcache = dict()   
        self.nlp = spacy.load("en_core_web_lg")
        self.osm_lookup_url = os.getenv('OSM_LOOKUP_URL','http://localhost:2322/api/?q=%s')

    
    
    # Helps in converting the first letter of each sentence to capital
    def preprocess(self, text):
        return text.title()
            
    # Function which helps in fetching the geotag from the source input file
    def get_geotag(self, text):
        gtag = []
        doc = self.nlp(text)
        for E in doc.ents:
            if E.label_ in ['GPE', 'LOC']:
                if self.output_fields == 'text':
                    gtag.append(E.text)
                elif self.output_fields == 'label':
                    gtag.append(E.label_)
                else:
                    gtag.append((E.text, E.label_))
        return gtag

    # Function to run the whole process first capitalising the first letter than finding geotags from input
            
    def process(self, text):
        ptext = self.preprocess(text)
        return self.get_geotag(ptext)       
        
                
    # Function which saves converted geo-coordinates in cache so the program should not contact OSM server for getting
    # information of repeated geotags
    def geo_cache(self, place):
        if place in self.gcache:
            return self.gcache[place]
        ## entry not found. update cache
        tags = self.osm_coordinates(place)
        self.gcache[place] = tags
        return tags
            
    
    
    # Connects to the service and check whether return value is city, country, state and town
    # from the input text which are tweets to fetch latitudes and longitudes
    def osm_coordinates(self, place):
        not_found = (None, None)
        req = requests.get(self.osm_lookup_url % place)
        try:
            data = req.json()
            for d in data['features']:
                if 'place' != d['properties']['osm_key']:
                    continue
                if not d['properties']['osm_value'] in ['country', 'city', 'county', 'village', 'town']:
                    continue
                lon = d['geometry']['coordinates'][0]
                lat = d['geometry']['coordinates'][1]
                return (lat, lon)
        except:
            ## errors when spacy return wrong names for places which has unsupported characters like '#Endomondo'
            return not_found
        return not_found
    
    
    
    # Each input line calls process function runs spacy to fetch geotags then convert them
    #into geo-coordinates and store it in mentions list and create require structure
    def parse_pandas(self,text,time):
        mentions = []
        plist = self.process(text)
        mentions.append(self.places_to_geo_coordinates(plist))

        try:
            result = self.create_output_struct(text,time,mentions)
        except Exception as err:
            print(err)
            print(traceback.print_exc())
            return []

        return(result)    
       
     # Extracts all geo-places checks for cache if converted lat and long is present for that particular
    #geotag if present accesses it else connect to service to get geo-coordinates

    def places_to_geo_coordinates(self, places):
        gtags = []
        for p in places:
            if len(p) > 0:
                tags = self.geo_cache(p)
                if not tags[0] is None: # filter out things which spacy incorrectly detected  as a place
                    gtags.append((p, tags[0], tags[1]))
        return gtags
            
    #To create a JSON format output strcture which makes it easy
            #for indexing in elastic search
    def create_output_struct(self, text,time, user_mentions):
        
       # result = {'media':'twitter'}
       # result['location']= {'geotag':{'lat': tab['y'], 'lon': tab['x']}}
        mentions = []
        results = []
        cur_mention = user_mentions[0]
        #print(user_mentions)
        #print(cur_mention)
        for m in cur_mention:
            mentions.append({'place': m[0], 'geotag':{'lat': m[1], 'lon': m[2]}})
        result[0] = mentions
        result[1] = time
        result[2] = 'twitter'
        #result['time of publish'] = time
        #result['raw_data'] = text
                
        return result
       

    def process_data(self,message) -> None:
        
        #print(message.value)        
        try:
            #-- Perform all the module logic here --#
            # To get value from a field (Example)
            util.json_util = util.JsonDataUtil(message.value)
            results = self.parse_pandas(util.json_util.get_value("text"),util.json_util.get_value("created_at"))
            util.json_util.add_metadata("media",results[2])
            util.json_util.add_metadata("time",results[1])
            util.json_util.add_metadata("mentions",results[0])
        
        except:
            print("Invalid Tweet Record.. Skipping")
            raise

        processes_message = util.json_util.get_json() 
                        
        return processes_message



if __name__ == "__main__":

    #processor_name: Unique processor name for the module, 
    #source_topic: Topic from which the module should accept the record to be processed, 
    # target_topic: Topic to which the module publishes the processed record
   s_topic = os.getenv('MODULE_SRC_TOPIC','lews-twitter')
   t_topic = os.getenv('MODULE_TGT_TOPIC','t_topic')
   proc_name = os.getenv('MODULE_NAME','Module01')
   
   
   run(ConKafkaInStreamProcessor(processor_name=proc_name, source_topic=s_topic, target_topic=t_topic))
