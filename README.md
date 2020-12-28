# DataStax Apache Pulsar Connector

An Apache Pulsar® sink for transferring events/messages from Kafka topics to Apache Cassandra®,
DataStax Astra or DataStax Enterprise (DSE).

## Installation

To download and install this connector please follow the procedure detailed [here](https://docs.datastax.com/en/pulsar/doc/pulsar/install/pulsarInstall.html).

## Documentation

All documentation is available online [here](https://docs.datastax.com/en/pulsar/doc/index.html).

## Building from the sources

If you want to develop and test the connector you need to build the jar from sources.
To do so please follow those steps:

1. First build the uber-jar: 

       mvn clean package

2. Open the Pulsar sink config file `config/pulsar-sink.yml`. Update the plugin 
   search path to include the uber-jar:

      TODO

3. Run Pulsar Sink LocalRun and specify the path to the that config file:

       bin/pulsar-admin sinks localrun --sink-config-file \
          config/pulsar-sink.yaml
      TODO

## Mapping specification

To see practical examples and usages of mapping, see:
https://docs.datastax.com/en/pulsar/doc/search.html?searchQuery=mapping 
