from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
from datetime import datetime

app = Flask(__name__)

# Initialiser le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

# Route API pour envoyer un événement à Kafka
@app.route('/evenement', methods=['POST'])
def envoyer_evenement():
    data = request.get_json()

    # Construire le message JSON
    evenement = {
        "isbn": data["isbn"],
        "action": data["action"],  
        "timestamp": datetime.utcnow().isoformat(),
        "login": data["login"]
    }

    # Envoyer à Kafka
    producer.send(
        topic="bibliotheque_prets_retours",
        key=data["isbn"],   
        value=evenement
    )


    return jsonify({"message": "Événement envoyé à Kafka avec succès"}), 200

# Lancer le serveur Flask
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
