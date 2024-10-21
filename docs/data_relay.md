# Data Relay Process

The Data Relay process facilitates secure and efficient data transfer from Caltrans network to Snowflake. 

This document provides an overview of the Data Relay process, setup instructions, and usage guidelines.

## Table of Contents
- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Pre-requisites](#pre-requisites)
- [Step-by-Step Guide](#step-by-step-guide)
  - [1. Configuration Files](#1-configuration-files)
  - [2. Setting Up Certificates](#2-setting-up-certificates)
  - [3. Relay Server Configuration](#3-relay-server-configuration)
  - [4. Elasticsearch Configuration](#4-elasticsearch-configuration)
  - [5. Running the Data Relay](#5-running-the-data-relay)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)

---

## Overview

The Data Relay process is designed to transfer data between systems. 


**Key Components**:
- **Data Puller**: Where data originates, such as Oracle or Snowflake databases.
- **Relay Servers**: Intermediate servers that handle the data transfer securely and relay it to the destination.
- **Uploader**: The final destination for data storage and search.

---

## System Architecture

1. **Source**: Data originates from a source database (e.g., Oracle, Snowflake).
2. **Relay Server**: The relay server securely transfers data using SSL/TLS certificates and passes it on to Elasticsearch.
3. **Destination**: The destination system, typically an Elasticsearch cluster, ingests the data for search and analytics.

![System Architecture Diagram](https://example.com/relay-architecture-diagram.png) <!-- Update with actual link to architecture diagram -->

---

## Pre-requisites

Before setting up the Data Relay process, ensure the following:

1. **Elasticsearch Setup**: Elasticsearch cluster is running and accessible.
2. **Certificates**: SSL/TLS certificates for secure communication between the data relay and Elasticsearch.
3. **Relay Server**: A properly configured server to act as the data relay.
4. **Database Access**: Proper access and credentials to the source database (e.g., Oracle, Snowflake).

---

## Step-by-Step Guide

### 1. Configuration Files

Create the necessary configuration files for both the data source and the destination.

- **Relay Configuration**: `relay-config.yml`
- **Elasticsearch Output Configuration**: `elasticsearch-output.yml`

These configurations define how data will be relayed from the source to the destination, including credentials and security details.

### 2. Setting Up Certificates

To ensure secure communication, configure SSL/TLS certificates.

1. Place the required certificates in `/usr/local/elasticsearch/bin/ca_ca.crt` on your relay server.
2. Make sure your Elasticsearch cluster is set up to accept SSL/TLS connections.

### 3. Relay Server Configuration

Ensure the relay server is properly configured:

- Define the relay server's address and ports.
- Verify that the relay server can securely communicate with the source and destination.

Edit the `relay-config.yml` as follows:
```yaml
relay:
  host: relay-server-host
  port: 9000
  ssl:
    enabled: true
    cert_path: /path/to/cert.crt
    key_path: /path/to/key.key
This is a placeholder sentence for the data relay documentation.



