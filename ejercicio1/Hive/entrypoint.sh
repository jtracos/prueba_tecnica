#!/bin/sh

export HADOOP_HOME=/opt/hadoop-3.2.0

export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar

export JAVA_HOME=/usr/local/openjdk-8

export METASTORE_DB_HOSTNAME=${METASTORE_DB_HOSTNAME:-localhost}

MYSQL='mysql'
POSTGRES='postgres'

if [ "${METASTORE_TYPE}" = "${MYSQL}" ]; then

    echo "Waiting for database on ${METASTORE_DB_HOSTNAME} to launch on 3306 ..."

    while ! nc -z ${METASTORE_DB_HOSTNAME} 3306; do
        sleep 1
    done

    echo "Database on ${METASTORE_DB_HOSTNAME}:3306 started"

    echo "Checking Hive Metastore schema..."

    if /opt/apache-hive-metastore-3.0.0-bin/bin/schematool \
        -info \
        -dbType mysql; then

        echo "Hive Metastore schema already initialized"

    else

        echo "Initializing Hive Metastore schema..."

        /opt/apache-hive-metastore-3.0.0-bin/bin/schematool \
            -initSchema \
            -dbType mysql

        if [ $? -ne 0 ]; then
            echo "ERROR: Hive Metastore schema initialization failed"
            exit 1
        fi

    fi

    echo "Starting Hive Metastore..."

    exec /opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore

fi