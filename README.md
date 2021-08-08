# Building a Streaming API to Evaluate Human Balance

## Project Overview
The Step Trending Electronic Data Interface (STEDI) application assesses fall risk for seniors. When a senior takes a test, they are scored using an index that reflects the likelihood of falling and potentially sustaining an injury in the course of walking. STEDI uses a Redis datastore for risk scores and other data. Using Spark, I aggregated Kafka Connect Redis Source events and Business Events to create a Kafka topic containing anonymized risk scores of seniors in the clinic and generated a population risk graph.