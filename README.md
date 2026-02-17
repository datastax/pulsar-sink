Dated: February 10, 2026

This repository was temporarily offline following the confirmation of unauthorized activity within a limited number of our public Datastax GitHub repositories, listed below. Working with our internal incident response team, we took steps to contain, remediate and investigate the activity.

We followed established incident-response processes to review and to revert any unauthorized activity.

Required Actions: Collaborators who interacted with this repository between January 31, 2026, and February 3, 2026, rebase your branch onto the new `main`/ `master`. Do not merge `main` / `master` into your branch.

At Datastax, we remain committed to your security and to transparency within the open-source community.

Impacted Repositories:

- https://github.com/datastax/cassandra-quarkus
- https://github.com/datastax/graph-examples
- https://github.com/datastax/metric-collector-for-apache-cassandra
- https://github.com/datastax/native-protocol
- https://github.com/datastax/pulsar-sink

Developers should review their environments for the following Indicators of Compromise in conjunction with downloading the impacted repositories:
- File SHA1 Hashes:
  - def338ee2fbc6f84b4a22ead67471824fe1a565f
  - 78be1ea752622c75fd5c636abc2e6e7a51484323
- File names:
  - .vscode/tasks.json
  - temp_auto_push.bat
- Domain names:
  - vscode-extension-260120.vercel[.]app
  - fullnode.mainnet.aptoslabs[.]com
  - api.trongrid[.]io
  - bsc-dataseed.binance[.]org
- IP addresses:
  - 23.27.20[.]143
  - 136.0.9[.]8
- The user agent for the git pushes was git/2.51.0.vfs.0.3-Windows

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
