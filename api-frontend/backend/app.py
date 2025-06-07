
# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify
from kafka import KafkaProducer
from hdfs import InsecureClient
from datetime import datetime
import json
import time
import os
from livre_service import fusionner_catalogue_stock
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Configuration Kafka
bootstrap_servers = 'kafka:9092'
topic = 'bibliotheque_prets_retours'

# Configuration HDFS
hdfs_client = InsecureClient('http://namenode:9870', user='hadoop')
stock_file_path = '/data/bibliotheque/stock_reel.json'

# Attente pour que Kafka soit prêt
time.sleep(10)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

REQUIRED_FIELDS = ['isbn', 'action', 'login']

# ----------- API EXISTANTES ------------

@app.route('/evenement', methods=['POST'])
def envoyer_evenement():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Aucune donnée reçue"}), 400

    missing = [f for f in REQUIRED_FIELDS if f not in data]
    if missing:
        return jsonify({"error": f"Champs manquants : {', '.join(missing)}"}), 400

    empty = [f for f in REQUIRED_FIELDS if not data.get(f)]
    if empty:
        return jsonify({"error": f"Champs vides : {', '.join(empty)}"}), 400

    try:
        data["timestamp"] = datetime.utcnow().isoformat()
        key = data.get('isbn')
        producer.send(topic, key=key.encode('utf-8'), value=data)
        producer.flush()
        return jsonify({"message": "Événement envoyé à Kafka"}), 200
    except Exception as e:
        return jsonify({"error": f"Erreur serveur : {str(e)}"}), 500

@app.route('/stock/<isbn>', methods=['GET'])
def get_stock(isbn):
    try:
        with hdfs_client.read(stock_file_path, encoding='utf-8') as reader:
            stock_dict = json.load(reader)
        if isbn in stock_dict:
            return jsonify({isbn: stock_dict[isbn]}), 200
        else:
            return jsonify({"error": "ISBN non trouvé"}), 404
    except Exception as e:
        return jsonify({"error": f"Erreur lecture stock : {str(e)}"}), 500

# -----------  ENDPOINTS API LIVRES ------------

@app.route("/api/livres")
def get_livres():
    page = int(request.args.get("page", 1))
    size = int(request.args.get("size", 10))
    livres_dict = fusionner_catalogue_stock()
    livres = list(livres_dict.values())
    total = len(livres)
    start = (page - 1) * size
    end = start + size
    return jsonify({
        "page": page,
        "size": size,
        "total": total,
        "livres": livres[start:end]
    })

@app.route("/api/livres/<isbn>")
def get_livre_by_isbn(isbn):
    livres_dict = fusionner_catalogue_stock()
    livres = list(livres_dict.values())
    livre = next((l for l in livres if l["isbn"] == isbn), None)
    if livre:
        return jsonify(livre)
    return jsonify({"error": "Livre non trouvé"}), 404

@app.route("/api/emprunter/<isbn>", methods=["POST"])
def emprunter_livre(isbn):
    livres_dict = fusionner_catalogue_stock()
    livre = livres_dict.get(isbn)
    if not livre:
        return jsonify({"error": "ISBN non trouvé"}), 404
    if livre["stock_actuel"] <= 0:
        return jsonify({"error": "Plus d'exemplaires disponibles"}), 400

    event = {
        "isbn": isbn,
        "action": "pret",
        "login": "user1",
        "quantite": 1,
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send(topic, key=isbn.encode('utf-8'), value=event)
    producer.flush()
    return jsonify({"message": "Emprunt enregistré"}), 200

@app.route("/api/retourner/<isbn>", methods=["POST"])
def retourner_livre(isbn):
    livres_dict = fusionner_catalogue_stock()
    if isbn not in livres_dict:
        return jsonify({"error": "ISBN inconnu"}), 404

    event = {
        "isbn": isbn,
        "action": "retour",
        "login": "user1",
        "quantite": 1,
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send(topic, key=isbn.encode('utf-8'), value=event)
    producer.flush()
    return jsonify({"message": "Retour enregistré"}), 200

@app.route("/api/statistiques/top-auteurs")
def top_auteurs():
    try:
        hdfs_stats_path = "/data/bibliotheque/top_3_auteurs/part-00000.csv"
        with hdfs_client.read(hdfs_stats_path, encoding="utf-8") as f:
            lines = f.readlines()

        auteurs = []
        for line in lines[1:]:  # Skip header
            if "," in line:
                auteur, total = line.strip().split(",")
                auteurs.append({
                    "auteur": auteur,
                    "nb_exemplaires": int(total)
                })

        return jsonify(auteurs), 200

    except Exception as e:
        return jsonify({"error": f"Erreur lecture stats depuis HDFS : {e}"}), 500

# -----------  LANCEMENT DE L'API ------------

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5550)
