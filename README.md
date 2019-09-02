# LEWS Data-pipeline Module Template

This is a template for developing pipeline modules.

## Pre-Requisites

- Docker 18.09.0 or higher

- Kafka Broker

- Streaming data in json format


## Running in local environment
### Install all dependancies
Install dependancies given in requirements.txt. You may add your dependancies in this file for you module
```bash
pip install -r requirements.txt
```
### Install and run Kafka Broker
#### Ubuntu 18.04
Follow https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-18-04
#### Windows 
Follow https://medium.com/@shaaslam/installing-apache-kafka-on-windows-495f6f2fd3c8
#### MacOS
Follow https://medium.com/pharos-production/apache-kafka-macos-installation-guide-a5a3754f09c

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
docker run -e KAFKA_BROKER="<kafka-broker-host:port>" \
-e MODULE_NAME="<module name>" \
-e MODULE_SRC_TOPIC="<source topic for the module>" \
-e MODULE_TGT_TOPIC="<target topic for the module>" lews-pipeline-<module name>
```
