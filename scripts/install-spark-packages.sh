#!/usr/bin/env bash

# Installs Spark packages.
# This is an alternative to configure_spark_with_delta_pip() (for Delta) to avoid downloading packages each time

set -eux

while getopts "p:" opt; do
    case $opt in
        p) packages+=("$OPTARG");;
    esac
done

# defaults
scalaVersion=2.12
kafkaClientsVersion=3.3

read -r pysparkLocation <<<$(python -c "import pkg_resources; print(pkg_resources.get_distribution('pyspark').location)") || true
read -r deltaVersion <<<$(python -c "import pkg_resources; print(pkg_resources.get_distribution('delta-spark').version)") || true
read -r pysparkVersion <<<$(python -c "import pkg_resources; print(pkg_resources.get_distribution('pyspark').version)") || true

# read latest available minor version for kafka-clients library
kafkaClientsPatchVersion=$(curl -s https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/maven-metadata.xml | grep $kafkaClientsVersion | sed -E 's/.*([[:digit:]]\.[[:digit:]]\.[[:digit:]]).*/\1/' | sort | tail -n1)

cd "$pysparkLocation"/pyspark/jars

# make sure this is the correct directory containing pyspark jars.
# This command will cause the script to fail if no *.jar files are in the current directory.
ls *.jar > /dev/null

if echo "${packages[@]}" | fgrep --word-regexp "delta"; then
    wget \
        https://repo1.maven.org/maven2/io/delta/delta-core_"$scalaVersion"/"$deltaVersion"/delta-core_"$scalaVersion"-"$deltaVersion".jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/"$deltaVersion"/delta-storage-"$deltaVersion".jar
fi
