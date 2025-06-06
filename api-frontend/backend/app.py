# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify
from kafka import KafkaProducer
from hdfs import InsecureClient
import json
import time
from datetime import datetime

app = Flask(__name__)

# Configuration Kafka
bootstrap_servers = 'kafka:9092'
topic = 'bibliotheque_prets_retours'

# Configuration HDFS
hdfs_client = InsecureClient('http://namenode:9870', user='hadoop')
stock_file_path = '/data/bibliotheque/stock_reel.json'

# Attente pour que Kafka soit prêt
time.sleep(10)

# Création du producer Kafka
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

REQUIRED_FIELDS = ['isbn', 'action', 'login']


def send_event_to_kafka(data):
    data["timestamp"] = datetime.utcnow().isoformat()
    key = data.get('isbn')
    producer.send(topic, key=key.encode('utf-8'), value=data)
    producer.flush()
    print("Message envoyé à Kafka:", data)


@app.route('/evenement', methods=['POST'])
def envoyer_evenement():
    data = request.get_json()

    if not data:
        return jsonify({"error": "Aucune donnée reçue"}), 400

    missing_fields = [field for field in REQUIRED_FIELDS if field not in data]
    if missing_fields:
        return jsonify({
            "error": f"Champs manquants : {', '.join(missing_fields)}"
        }), 400

    empty_fields = [field for field in REQUIRED_FIELDS if not data.get(field)]
    if empty_fields:
        return jsonify({
            "error": f"Champs vides : {', '.join(empty_fields)}"
        }), 400

    try:
        send_event_to_kafka(data)
        return jsonify({"message": "Événement envoyé à Kafka"}), 200
    except Exception as e:
        return jsonify({"error": f"Erreur serveur : {str(e)}"}), 500


@app.route('/stock/<isbn>', methods=['GET'])
def get_stock(isbn):
    try:
        # Lecture en temps réel du fichier JSON sur HDFS
        with hdfs_client.read(stock_file_path, encoding='utf-8') as reader:
            stock_dict = json.load(reader)

        # Recherche de l'ISBN demandé
        if isbn in stock_dict:
            return jsonify({isbn: stock_dict[isbn]}), 200
        else:
            return jsonify({"error": "ISBN non trouvé"}), 404
    except Exception as e:
        return jsonify({"error": f"Erreur lecture stock : {str(e)}"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5550)
