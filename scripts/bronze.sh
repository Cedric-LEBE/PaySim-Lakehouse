#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
export SPARK_LOCAL_IP=127.0.0.1
export PYTHONPYCACHEPREFIX="${TMPDIR:-/tmp}/banking-pycache"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export SPARK_HOME="$(.venv/bin/python -c 'import importlib.util, os; print(os.path.dirname(importlib.util.find_spec("pyspark").origin))')"
mkdir -p data/spark/ivy data/spark/local
artifacts=(
  com.google.code.findbugs_jsr305-3.0.0.jar
  io.delta_delta-spark_2.13-4.0.1.jar io.delta_delta-storage-4.0.1.jar
  org.antlr_antlr4-runtime-4.13.1.jar org.apache.commons_commons-pool2-2.12.0.jar
  org.apache.hadoop_hadoop-client-api-3.4.1.jar org.apache.hadoop_hadoop-client-runtime-3.4.1.jar
  org.apache.kafka_kafka-clients-3.9.1.jar
  org.apache.spark_spark-sql-kafka-0-10_2.13-4.0.1.jar
  org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.0.1.jar
  org.lz4_lz4-java-1.8.0.jar
  org.scala-lang.modules_scala-parallel-collections_2.13-1.2.0.jar
  org.slf4j_slf4j-api-2.0.16.jar org.xerial.snappy_snappy-java-1.1.10.7.jar
)
deps=(--packages io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1)
ready=true
jars=''
for artifact in "${artifacts[@]}"; do
  path="$PWD/data/spark/ivy/jars/$artifact"
  if [ ! -s "$path" ]; then ready=false; break; fi
  jars="${jars:+$jars,}$path"
done
if $ready; then deps=(--jars "$jars"); fi
exec .venv/bin/spark-submit --master 'local[2]' --driver-memory 768m \
  --conf "spark.jars.ivy=$PWD/data/spark/ivy" \
  --conf "spark.local.dir=$PWD/data/spark/local" \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.ui.enabled=false "${deps[@]}" \
  "${BANKING_SPARK_APP:-src/banking/bronze.py}" "$@"
