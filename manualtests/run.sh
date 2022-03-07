#bash
# This is a sample file that starts the Sink using the locally built NAR files
 
set -x
pulsar-admin sinks localrun --name astra -i people -a $(realpath ../pulsar-dist/target/cassandra-enhanced-pulsar-sink-*-nar.nar) --sink-config-file $(realpath testastra.yml)

