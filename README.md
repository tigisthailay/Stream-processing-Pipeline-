# 📈 Stream Processing Pipeline

A real-time financial data processing pipeline built with **Finnhub WebSocket**, **Apache Kafka**, **Apache Spark**, **Snowflake**, and **Docker**. This project demonstrates end-to-end streaming data ingestion, processing, and storage.

---

## Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Data Flow](#-data-flow)
- [Monitoring & Logging](#-monitoring--logging)
- [Screenshots](#-screenshots)
- [Contributing](#-contributing)
- [Contact](#-contact)

---

## Overview

This pipeline captures and processes live financial data in real time. It uses:

- **Apache Kafka** for messaging
- **Apache Spark** for stream processing
- **Snowflake** for data warehousing
- **Docker Compose** for container orchestration
- **Finnhub WebSocket API** for real-time data feed

---

## 🛠️ Architecture

<img title=" " alt="Alt text" src="/screenshots/pipeline.png" width= "1000">

## Project Structure

```
.
├── Finnhub_Producer/         # Streams data from Finnhub WebSocket API
├── stream_processing/        # Apache Spark streaming job
├── docker-compose.yaml       # Docker services configuration
├── screenshots/              # Architecture diagrams and screenshots
├── .env                      # Environment variables
└── README.md
```

---

## ⚙️ Getting Started

### Prerequisites

Ensure you have the following installed:

- [Docker & Docker Compose](https://www.docker.com/)
- [Python 3.8+](https://www.python.org/)
- [Apache Spark](https://spark.apache.org/)
- [Snowflake Account](https://signup.snowflake.com/)
- [Finnhub API Key](https://finnhub.io/)

### Installation

1. **Clone the Repository**:

```bash
git clone https://github.com/tigisthailay/Stream-processing-Pipeline-.git
cd Stream-processing-Pipeline-
```

2. **Add Environment Variables**:

Create a `.env` file and add:

```env
KAFKA_BOOTSTRAP_SERVER=your_kafka_bootstap_server
KAFKA_TOPIC_NAME=your kafka_topic
KAFKA_SINK_TOPIC=your_kafka_sink_topic
FINNHUB_API_KEY=your_finnhub_api_key
FINNHUB_Symbols=list of symbols eg. AAPL,AMZN
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
```

3. **Run the Pipeline**:

```bash
docker-compose up --build
```

---

## Data Flow

1. **Ingestion**: Real-time data is fetched from Finnhub and sent to Kafka.
2. **Processing**: Spark reads Kafka streams and transforms the data.
3. **Storage**: Processed data is written to Snowflake.

---

## Monitoring & Logging

- **Kafka UI**: `http://localhost:8083`
- **Docker Logs**:

```bash
docker-compose logs -f
```

---

## Screenshots

> Include any visual here (e.g., `pipeline_architecture.png`)

```
screenshots/
├── pipline.png
```

---

## CONTRIBUTERS
- [**Tegisty Hailay**](https://github.com/tigisthailay)

## Contact

For questions or feedback, feel free to contact [Tegisty Hailay](tigisthay13@gmail.com).