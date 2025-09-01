# open-table-data-challenge
ata Science Spark k8s

This project demonstrates a simple data pipeline using Kafka, Apache Spark (batch + streaming), and Python producers/consumers deployed on Kubernetes (Minikube). The pipeline includes sending messages from a producer, consuming them with a consumer, and processing with Spark jobs.


Project Structure

.
├── app
│   ├── create_topics.py       # Script to create Kafka topics
│   ├── kafka_consumer.py      # Kafka consumer application
│   ├── kafka_producer.py      # Kafka producer application
│   ├── spark_batch.py         # Spark batch processing job
│   ├── spark_streaming.py     # Spark structured streaming job
│   ├── test_consumer.py       # Local test consumer
│   └── test_producer.py       # Local test producer
├── docker-compose.yaml        # Local Kafka + Zookeeper setup
├── Dockerfile                 # Custom image for Python + Spark apps
├── helmfile.yaml              # Helm deployments for Kafka/Zookeeper
├── k8s                        # Kubernetes manifests
│   ├── create-topic.yml       # Job: Create Kafka topics
│   ├── kafka-consumer.yaml    # Deployment: Kafka consumer
│   ├── kafka-producer.yaml    # Deployment: Kafka producer
│   ├── spark-batch.yaml       # Job: Spark batch processing
│   └── spark-stream.yaml      # Deployment: Spark streaming job
├── kafka
│   ├── kafka.yml              # Kafka deployment/service
│   └── zookeeper.yml          # Zookeeper deployment/service
├── requirements.txt           # Python dependencies


## Kubernetes Manifests Overview

| Manifest                | Purpose                                                 |
| ----------------------- | ------------------------------------------------------- |
| k8s/create-topic.yml    | Job that creates required Kafka topics once.            |
| k8s/kafka-producer.yaml | Deployment that continuously sends messages to Kafka.   |
| k8s/kafka-consumer.yaml | Deployment that continuously reads messages from Kafka. |
| k8s/spark-batch.yaml    | Job that runs Spark batch processing once.              |
| k8s/spark-stream.yaml   | Deployment that runs Spark streaming job continuously.  |
| kafka/zookeeper.yml     | Deployment + Service for Zookeeper cluster.             |
| kafka/kafka.yml         | Deployment + Service for Kafka broker(s).               |

## Python Scripts Overview

| Script                 | Purpose                                        |
| ---------------------- | ---------------------------------------------- |
| app/create_topics.py   | Creates Kafka topics from Python.              |
| app/kafka_producer.py  | Sends messages to Kafka continuously.          |
| app/kafka_consumer.py  | Consumes messages from Kafka continuously.     |
| app/spark_batch.py     | Runs a Spark batch job on Kafka data.          |
| app/spark_streaming.py | Runs Spark structured streaming on Kafka data. |
| app/test_producer.py   | Local producer for testing without k8s.        |
| app/test_consumer.py   | Local consumer for testing without k8s.        |


Prerequisites

Make sure the following are installed:
	-	Minikube - https://minikube.sigs.k8s.io/docs/start/?arch=%2Fmacos%2Farm64%2Fstable%2Fbinary+download
	-	kubectl - https://kubernetes.io/docs/tasks/tools/
	-	Helm - https://helm.sh/docs/intro/install/
	-	Docker - https://docs.docker.com/engine/install/
	-   Helmfile - https://github.com/helmfile/helmfile
	-   K9s - https://k9scli.io/topics/install/ 



Setup

1. Start Minikube

minikube start


1. Build Docker Image

docker build -t pyspark-stream:latest .


3. Deploy Zookeeper & Kafka

You can use Helmfile or raw manifests:

Using Helmfile
helmfile init
helmfile diff
helmfile apply

Using manifests

kubectl apply -f kafka/zookeeper.yml
kubectl apply -f kafka/kafka.yml

Check that pods are running:

kubectl get pods


⸻

4. Create Kafka Topics

Run the Job to create topics:

kubectl apply -f k8s/create-topic.yml
kubectl logs -f job/kafka-create-topic


5. Deploy Producer & Consumer

Producer

kubectl apply -f k8s/kafka-producer.yaml

Consumer

kubectl apply -f k8s/kafka-consumer.yaml

Check logs to verify messages are sent and received:

kubectl logs -f deployment/kafka-producer
kubectl logs -f deployment/kafka-consumer


6. Run Spark Jobs

Batch Job (runs once)

kubectl apply -f k8s/spark-batch.yaml
kubectl logs -f job/spark-batch

Streaming Job (runs continuously)

kubectl apply -f k8s/spark-stream.yaml
kubectl logs -f deployment/spark-stream


How to Verify it Works
	1.	Kafka topics exist:

kubectl exec -it <kafka-pod-name> -- kafka-topics --bootstrap-server kafka-service:9092 --list

	2.	Producer logs: should show messages being sent.
	3.	Consumer logs: should show messages being received.
	4.	Spark batch output: check the output path defined in spark_batch.py.
	5.	Spark streaming logs: should show messages being processed continuously.


## Development & Testing

Run Kafka locally with Docker Compose

docker-compose up -d

Run producer and consumer locally:

python app/test_producer.py
python app/test_consumer.py


⸻

Useful Commands

Get all pods:

kubectl get pods -n default

Delete all deployments and jobs:

kubectl delete -f k8s/
kubectl delete -f kafka/

Stop Minikube:

minikube stop



# Reference
# https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/
