# DataStax Apache Pulsar Connector

An Apache Pulsar® sink for transferring events/messages from Pulsar topics to Apache Cassandra®,
DataStax Astra or DataStax Enterprise (DSE) tables.

## Installation

To download and install this connector please follow the procedure detailed [here](https://docs.datastax.com/en/pulsar-connector/docs/pulsarInstall.html).

## Documentation

All documentation is available online [here](https://docs.datastax.com/en/pulsar-connector/docs/index.html).

## Building from the sources

If you want to develop and test the connector you need to build the jar from sources.
To do so please follow those steps:

1. First build the package and locate the cassandra-enhanced-pulsar-sink .nar file (in pulsar-dist/target):

       mvn clean package

2. Copy the .nar file into the 'connectors' directory of your Pulsar broker

2. Open the Pulsar Sink config file `config/pulsar-sink.yml` and configure your topic and mapping:

       - in "topics" put the name of the topic
       - create a mapping from your topic to your Cassandra table

3. Run Pulsar Sink LocalRun and specify the path to the that config file:

       bin/pulsar-admin sinks localrun \
          --sink-config-file /path/to/config/pulsar-sink.yaml \
          -t cassandra-enhanced \
          -i persistent://public/default/topic

   With this command you are reading data from topic 'persistent://public/default/topic', the destination Cassandra cluster (address, authentication...), keyspace
   and table are defined in pulsar-sink.yml file

   You can map multiple topics to multiple tables.

## Mapping specification

To see practical examples and usages of mapping, see:
https://docs.datastax.com/en/pulsar-connector/docs/cfgPulsarMapTopicTable.html
