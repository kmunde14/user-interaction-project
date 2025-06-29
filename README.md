# User Interaction Streaming Pipeline (Kafka + Python)

This project simulates and processes user interaction events in real-time using **Apache Kafka**, **Python**, **SQLite**, and **pandas**.

##  Features

- Produces fake user interaction data (clicks, views, purchases).
- Streams data to a Kafka topic using `kafka-python`.
- Consumes and saves it to:
  - `interactions.json`
  - SQLite database (`interaction.db`)
- Visualizes interactions using a line graph.

---

## Tech Stack

- Python 3
- Kafka 3.6.1
- ZooKeeper
- Docker + Docker Compose
- Pandas & Matplotlib
- SQLite

---

## 🗂️ Project Structure

