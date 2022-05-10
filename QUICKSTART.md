# Docker Quick Start

## Build docker images

Build the project:

    mvn clean install -DskipTests

Build Pulsar image with the Pulsar Sink connector:
    
    docker build -t myrepo/apachepulsar-pulsar-sink:latest \
        -f docker/pulsar-with-sink/Dockerfile \
        --build-arg PULSAR_IMAGE=apache/apachepulsar \
        --build-arg PULSAR_TAG=2.8.3 \
        pulsar-dist/target


or with Datastax Luna Streaming:

    docker build -t myrepo/apachepulsar-pulsar-sink:latest \
        -f docker/pulsar-with-sink/Dockerfile \
        --build-arg PULSAR_IMAGE=datastax/lunastreaming \
        --build-arg PULSAR_TAG=2.10_0.1 \
        pulsar-dist/target


## Start Cassandra and Pulsar

Start containers for Cassandra 3.11 (c3), Cassandra 4.0 (c4), or DSE 6.8.16+ (dse4) at your convenience, and Apache Pulsar:

    CASSANDRA_IMAGE=cassandra:4.0.1 docker-compose up -d cassandra
    PULSAR_IMAGE=myrepo/apachepulsar-pulsar-sink:latest docker-compose up -d pulsar

Create the keyspace and table:

    docker exec -it cassandra cqlsh -e "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"
    docker exec -it cassandra cqlsh -e "CREATE TABLE ks1.table1 (name text, PRIMARY KEY (name))"

Deploy a Pulsar Sink Connector in the pulsar container:

    docker exec -it pulsar bin/pulsar-admin sinks create \
    --sink-type cassandra-enhanced \
    --tenant public \
    --namespace default \
    --name pulsar-sink-ks1-table1 \
    --inputs pulsar-topic-ks1-table1 \
    --sink-config '{
        "tasks.max": 1,
        "topics": "pulsar-topic-ks1-table1",
        "contactPoints": "cassandra",
        "loadBalancing.localDc": "datacenter1",
        "port": 9042,
        "cloud.secureConnectBundle": null,
        "ignoreErrors": "None",
        "maxConcurrentRequests": 500,
        "maxNumberOfRecordsInBatch": 32,
        "queryExecutionTimeout": 30,
        "connectionPoolLocalSize": 4,
        "jmx": true,
        "compression": "None",
        "auth": {
            "provider": "None",
            "username": null,
            "password": null,
            "gssapi": {
                "keyTab": null,
                "principal": null,
                "service": "dse"
            }
        },
        "ssl": {
            "provider": "None",
            "hostnameValidation": true,
            "keystore": {
                "password": null,
                "path": null
            },
            "openssl": {
                "keyCertChain": null,
                "privateKey": null
            },
            "truststore": {
            "password": null,
            "path": null
            },
            "cipherSuites": null
        },
        "topic": {
            "pulsar-topic-ks1-table1": {
                "ks1": {
                    "table1": {
                        "mapping": "name=value.name",
                        "consistencyLevel": "LOCAL_ONE",
                        "ttl": -1,
                        "ttlTimeUnit": "SECONDS",
                        "timestampTimeUnit": "MICROSECONDS",
                        "nullToUnset": true,
                        "deletesEnabled": true
                    }
                },
                "codec": {
                    "locale": "en_US",
                    "timeZone": "UTC",
                    "timestamp": "CQL_TIMESTAMP",
                    "date": "ISO_LOCAL_DATE",
                    "time": "ISO_LOCAL_TIME",
                    "unit": "MILLISECONDS"
                }
            }
        }
    }
    '
   
Check the sink connector status (should be running):

    docker exec -it pulsar bin/pulsar-admin sink status --name pulsar-sink-ks1-table1

Check the sink connector logs:

    docker exec -it pulsar cat /pulsar/logs/functions/public/default/pulsar-sink-ks1-table1/pulsar-sink-ks1-table1-0.log

Write data to the topic:

    docker exec -it pulsar bin/pulsar-client produce \
        -vs 'json:{"type": "record","namespace": "com.example","name": "TestSchema","fields": [{ "name": "name", "type": "string"}]}' \
        -m '{"name": "myname"}' \
        pulsar-topic-ks1-table1

Check data on Cassandra:

    docker exec -it cassandra cqlsh -e "SELECT * FROM ks1.table1"

## Shutdown containers

    docker-compose down
