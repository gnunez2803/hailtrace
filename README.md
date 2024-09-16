# NOAA Data Collector

This system uses three services:
- data collector from NOAA database written in NodeJS and Typescript that pushes it to Kafka topic "raw-weather-reports"
- data etl processor written in Golang that consume Kafka messages from topic "raw-weather-reports", standardizes it, and pushes it to "transformed-weather-data"
- database injector and REST API service written in Golang.

![alt text](high-level-diagram.png "High Level")
