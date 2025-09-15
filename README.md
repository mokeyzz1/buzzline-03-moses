# buzzline-03-moses


This project demonstrates streaming data in Python with Apache Kafka.  
It includes multiple custom producers and consumers that generate, send, and process messages in real-time.  
Each task builds on the previous one, adding new features and complexity.  

---

## Setup

Clone the repo and set up your virtual environment (Python 3.11 recommended).

```bash
git clone https://github.com/mokeyzz1/buzzline-03-moses.git
cd buzzline-03-moses
python3 -m venv .venv
source .venv/bin/activate    # Mac/Linux
.venv\Scripts\activate       # Windows
pip install -r requirements.txt
Make sure Kafka and ZooKeeper are installed and running:

Terminal A — ZooKeeper

bash
Copy code
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
Terminal B — Kafka broker

bash
Copy code
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
Task 1: Basic Producer & Consumer
Producer: generates log-style messages continuously.

Consumer: tails the log, detects alerts, prints analytics.

Run:

bash
Copy code
python producers/basic_producer_case.py
python consumers/basic_consumer_case.py
Task 2: Custom Logging & Utilities
Added centralized logging (logs/project_log.log)

Added reusable utilities in utils/ for cleaner producer/consumer code.

Run:

bash
Copy code
python producers/log_producer_case.py
python consumers/log_consumer_case.py
Task 3: CSV Producer & Consumer
Producer reads from a CSV file (data/smoker_temps.csv) and streams rows into Kafka.

Consumer listens for messages and processes them in real-time.

Run:

bash
Copy code
python producers/csv_producer_case.py
python consumers/csv_consumer_case.py
Task 4: Custom MK Producers & Consumers
Created unique producer/consumer scripts under your identifier (MK).
This demonstrates customizing Kafka pipelines with your own code.

Producer: csv_producer_mk.py

Consumer: csv_consumer_mk.py

Run:

bash
Copy code
python -m producers.csv_producer_mk
python -m consumers.csv_consumer_mk
Task 5: README Updates (JSON + CSV MK Scripts)
JSON Producer & Consumer
A new JSON producer generates structured JSON messages.
The paired consumer processes and logs those messages.

Run:

bash
Copy code
python -m producers.json_producer_mk
python -m consumers.json_consumer_mk
CSV Producer & Consumer (MK Version)
The custom CSV producer (csv_producer_mk.py) reads sensor data from a CSV and streams to Kafka.
The paired CSV consumer (csv_consumer_mk.py) listens and processes rows in real-time.

Run:

bash
Copy code
python -m producers.csv_producer_mk
python -m consumers.csv_consumer_mk