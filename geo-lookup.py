from abc import ABC, abstractmethod
from kafka import KafkaConsumer, KafkaProducer
import json
import os
import LEWSJsonUtil as util
import spacy
import time
import requests

s_kafka_servers = os.environ.get("SOURCE_KAFKA_BOOTSTRAP_SERVERS","localhost:9092").split(",")
t_kafka_servers = os.environ.get("TARGET_KAFKA_BOOTSTRAP_SERVERS","localhost:9092").split(",")
s_topic = os.environ.get('MODULE_SRC_TOPIC','lews-twitter')
t_topic = os.environ.get('MODULE_TGT_TOPIC','t_topic')
proc_name = os.environ.get('MODULE_NAME','Module01')
osm_lookup_url = os.environ.get('OSM_LOOKUP_URL','https://photon.komoot.de/api/?q=%s')

print("Environment variables:")
print(f"SOURCE_KAFKA_BOOTSTRAP_SERVERS = {s_kafka_servers}")
print(f"TARGET_KAFKA_BOOTSTRAP_SERVERS = {t_kafka_servers}")
print(f"MODULE_SRC_TOPIC = {s_topic}")
print(f"MODULE_TGT_TOPIC = {t_topic}")
print(f"MODULE_NAME = {proc_name}")
print(f"OSM_LOOKUP_URL = {osm_lookup_url}")

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
            
           # try:
                self.processed_record = self.process_data(message)
                self.produce_data_kafka(self.processed_record)
           # except Exception as e:
           #     print("Skipping Record..",str(e))

        
            




    def __init__(self,processor_name,source_topic,target_topic,nlp_object):

        self.nlp_object = nlp_object 

        self.processor_name = processor_name
        
        self.source_topic = source_topic
        
        self.target_topic = target_topic
        
        self.s_bootstrap_servers = s_kafka_servers 

        self.t_bootstrap_servers = t_kafka_servers 
        #self.bootstrap_servers = 'localhost:9092'
        
        print("Initializing Kafka In-Stream Processor Module")
        
        self.consumer = KafkaConsumer(source_topic,group_id = self.processor_name, bootstrap_servers = self.s_bootstrap_servers,value_deserializer=lambda m: json.loads(m.decode('utf-8')))

       # self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)

        self.producer = KafkaProducer(bootstrap_servers=self.t_bootstrap_servers, value_serializer = lambda v: json.dumps(v).encode('utf-8'))




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

#------------------------- Geo Lookup Main Code --------------------------------------#
class NlpResearch:
    def __init__(self):
        self.chunksize = 1000
        self.output_fields = 'text'
        self.gcache = dict()
        self.url_template = osm_lookup_url
        # self.url_template = "http://localhost:2322/api/?q=%s"
        self.nlp = spacy.load("en_core_web_sm")

    # ----Spacy Functions --------------
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

    # -------------------------------------------------------------

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
        out = []
        response = requests.get(self.url_template % place)
        try:
            data = response.json()
            for d in data['features']:
                name = d['properties']['name']
                lon = d['geometry']['coordinates'][0]
                lat = d['geometry']['coordinates'][1]
                out.append((name, lon, lat))
    
        except Exception as err:
            ## errors when spacy return wrong names for places which has unsupported characters like '#Endomondo'
            print(err)
        return out

    # Each input line calls process function runs spacy to fetch geotags then convert them
    # into geo-coordinates and store it in mentions list and create require structure
    def parse_input(self, data):
        text = data['text']
        timestamp = data['timestamp_ms']
        plist = self.process(text)
        mentions = self.places_to_geo_coordinates(plist)

        try:
            result = self.create_output_struct(text, timestamp, mentions)
        except Exception as err:
            print(err)
            print(traceback.print_exc())
            return []

        return (result)

    # Extracts all geo-places checks for cache if converted lat and long is present for that particular
    # geotag if present accesses it else connect to service to get geo-coordinates

    def places_to_geo_coordinates(self, places):
        gtags = []
        for p in places:
            if len(p) > 0:
                tags = self.geo_cache(p)
                if 0 < len(tags):  # filter out things which spacy incorrectly detected  as a place
                    gtags.append((p, tags))
        return gtags

    # To create a JSON format output strcture which makes it easy
    # for indexing in elastic search
    def create_output_struct(self, text, timestamp, user_mentions):
        result = {'media': 'twitter'}
        result['time of publish'] = timestamp
        result['raw_data'] = text
        mentions = []
        for m in user_mentions:
            for rec in m[1]:
                mentions.append({'place': rec[0], 'geotag': {'lat': rec[1], 'lon': rec[2]}})
        result['mentions'] = mentions

        return result

    # Pandas data analytics framework to access data easily
    def process_input(self):
        f = open('test.txt', 'r')
        input = f.read()
        result = self.parse_input(json.loads(input))
        return result


#------------------------- Geo Lookup Main Code Ends Here ----------------------------#


class ConKafkaInStreamProcessor(AbstractKafkaInStreamProcessor):

     def process_data(self,message) -> None:
#------------------- Add module Logic in this section ---------------------#
        try:
            #-- Perform all the module logic here --#

            # To get value from a field (Example)
            util.json_util = util.JsonDataUtil(message.value)
            

            tweet_text = util.json_util.get_value("text")
            #Do Processing

            plist = self.nlp_object.process(tweet_text)
            mentions = self.nlp_object.places_to_geo_coordinates(plist) 



            #Adding metadata to the record (Example)
            util.json_util.add_metadata("mentions",json.dumps(mentions))
            #util.json_util.add_metadata("Longitude","-1.617780")

        except:
            print("Invalid Tweet Record.. Skipping")
            raise

        #Get the processed record with metadata added
        processes_message = util.json_util.get_json()
        #print("Processes Message:",json.dumps(processes_message))
        #time.sleep(1)
#---------------------- Add module logic in this section (End) ----------------------#
        return processes_message





if __name__ == "__main__":

    #processor_name: Unique processor name for the module, 
    #source_topic: Topic from which the module should accept the record to be processed, 
    # target_topic: Topic to which the module publishes the processed record
   s_topic = os.getenv('MODULE_SRC_TOPIC','lews-twitter')
   t_topic = os.getenv('MODULE_TGT_TOPIC','t_topic')
   proc_name = os.getenv('MODULE_NAME','Module01')

   nlp_obj = NlpResearch()
   
   run(ConKafkaInStreamProcessor(processor_name=proc_name, source_topic=s_topic, target_topic=t_topic, nlp_object=nlp_obj))