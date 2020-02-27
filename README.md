# LEWS Data-pipeline Module for Geo-Lookup

This is a template for developing pipeline modules.

### Running the module
Install dependancies given in requirements.txt. Add all module dependancies in this file
```bash
pip install -r requirements.txt
```

Running
```bash
python Kafka-stream-process.py
```

## Running in Docker (Recommended for Production)
### Building the Docker Image


```bash
docker build --tag lews-pipeline-<module name> .
```

### Usage

```bash
docker run -e SOURCE_KAFKA_BOOTSTRAP_SERVERS="<kafka-broker-host:port>" \
-e TARGET_KAFKA_BOOTSTRAP_SERVERS="<<kafka-broker-host:port>>" \
-e MODULE_NAME="Geo-Lookup" \
-e MODULE_SRC_TOPIC="lews-twitter" \
-e MODULE_TGT_TOPIC="es_sync" \
-e OSM_LOOKUP_URL="<OSM Template URL>" lews-pipeline-geo-lookup
```
