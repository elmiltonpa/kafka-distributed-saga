#!/bin/sh
set -e

echo "=================================================="
echo "Esperando que Apache Kafka este listo..."
echo "=================================================="

until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:29092 > /dev/null 2>&1; do
  echo "Kafka todavia no responde en kafka:29092. Reintentando en 2 segundos..."
  sleep 2
done

echo "Kafka esta activo y respondiendo."
echo "=================================================="
echo "Creando topicos de negocio y DLQs con 3 particiones..."
echo "=================================================="

TOPICS="activation.requested billing.events provisioning.events activation.events activation.requested.dlq billing.events.dlq provisioning.events.dlq activation.events.dlq"

for topic in $TOPICS; do
  echo "Configurando topico: $topic"
  /opt/kafka/bin/kafka-topics.sh --create \
    --if-not-exists \
    --bootstrap-server kafka:29092 \
    --partitions 3 \
    --replication-factor 1 \
    --topic "$topic"
done

echo "=================================================="
echo "Listado de topicos creados en el cluster:"
echo "=================================================="
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:29092
echo "Inicializacion de topicos completada con exito."
