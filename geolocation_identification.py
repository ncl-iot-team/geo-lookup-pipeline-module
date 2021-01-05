import os
import traceback
import spacy
import requests
from procstream import StreamProcessMicroService
import logging as logger

config = {"OSM_LOOKUP_URL" : os.environ.get('OSM_LOOKUP_URL', 'https://photon.komoot.de/api/?q=%s'),
                  "MODULE_NAME": os.environ.get('MODULE_NAME', 'LEWS_GEOLOOKUP'),
                  "CONSUMER_GROUP": os.environ.get("CONSUMER_GROUP", 'LEWS_GEOLOOKUP_CG')}
# ------------------------- Geo Lookup Main Code --------------------------------------#
class NlpResearch:
    def __init__(self):
        self.chunksize = 1000
        self.output_fields = 'text'
        self.gcache = dict()
        self.url_template = config.get("OSM_LOOKUP_URL")
        # self.url_template = "http://localhost:2322/api/?q=%s"
        self.nlp = spacy.load("en_core_web_lg")

    # ----Spacy Functions --------------
    # Helps in converting the first letter of each sentence to capital
    def preprocess(self, text):
        return text.title()

    # Function which helps in fetching the geotag from the source input file
    def get_geotag(self, text):
        gtag = []
        doc = self.nlp(text)
        print(f"Entities:{doc.ents}")
        for E in doc.ents:
            print(f"E.label_:{E.label_}")
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
    def create_output_struct(self, user_mentions):

        mentions = []
        for m in user_mentions:
            for rec in m[1]:
                # mentions.append({'place': rec[0], 'location': {'lat': rec[2], 'lon': rec[1]}})
                mentions.append({'lat': rec[2], 'lon': rec[1]})
        result = mentions
        return result


# ------------------------- Geo Lookup Main Code Ends Here ----------------------------#

class StreamProcessGeoLocationIdentificationService(StreamProcessMicroService):
    def __init__(self, config_new, gelocation_identifier_obj):
        super().__init__(config_new)
        self.gelocation_identifier_obj = gelocation_identifier_obj

    def process_message(self, message):
        payload = message.value
        try:
            original_text = payload.get('text')
            plist = self.gelocation_identifier_obj.process(original_text)
            user_mentions = self.gelocation_identifier_obj.places_to_geo_coordinates(plist)
            mentions = self.gelocation_identifier_obj.create_output_struct(user_mentions)
            # Adding metadata to the record (Example)

            if len(mentions) > 0:
               # payload['lews_meta_user_mentions_location_names'] = user_mentions
                payload['lews_meta_mentions_location'] = mentions[0]
        except:
            logger.error(f"Cannot lookup geolocation:{payload}")
            #raise
        print(payload)
        return payload

def main():
    nlp_obj = NlpResearch()
    geolocation_service = StreamProcessGeoLocationIdentificationService(config, nlp_obj)
    geolocation_service.start_service()


if __name__ == "__main__":
    main()
