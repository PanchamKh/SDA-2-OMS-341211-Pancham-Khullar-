@echo off
REM Creates the six OMS topics. Usage: create_topics.bat sda-kafka-1
set CONTAINER=%1
if "%CONTAINER%"=="" set CONTAINER=sda-kafka-1

for %%T in (orders-topic payments-topic fulfillment-topic inventory-topic returns-topic crm-events-topic) do (
  docker exec -it %CONTAINER% kafka-topics.sh --create --topic %%T --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3
)
docker exec -it %CONTAINER% kafka-topics.sh --list --bootstrap-server localhost:9092
