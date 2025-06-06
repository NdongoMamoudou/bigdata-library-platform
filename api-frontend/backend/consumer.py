from kafka import KafkaConsumer
import json
from hdfs import InsecureClient
import time

# Initialiser client HDFS (ajuste l'URL et user selon ta config)
hdfs_client = InsecureClient('http://namenode:9870', user='hadoop')

# Charger le stock initial depuis HDFS (le fichier JSON)
with hdfs_client.read('/data/bibliotheque/stock_reel.json', encoding='utf-8') as reader:
    stock_dict = json.load(reader)

print("Stock initial chargé, nombre de livres :", len(stock_dict))

# Kafka consumer config
consumer = KafkaConsumer(
    'bibliotheque_prets_retours',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='stock-update-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def update_stock(event):
    isbn = event.get('isbn')
    action = event.get('action')  # <-- action, pas type_action
    
    quantite = event.get('quantite', 1)
    try:
        quantite = int(quantite)
        if quantite < 1:
            quantite = 1
    except:
        quantite = 1

    if isbn not in stock_dict:
        stock_dict[isbn] = {'stock_initial': 0, 'stock_actuel': 0}

    if action == 'pret':
        stock_dict[isbn]['stock_actuel'] = max(stock_dict[isbn]['stock_actuel'] - quantite, 0)
    elif action == 'retour':
        stock_dict[isbn]['stock_actuel'] += quantite
    else:
        print(f"Action inconnue : {action}")

last_save_time = time.time()
save_interval = 30  # secondes

print("En attente de messages Kafka pour mise à jour du stock...")

for message in consumer:
    try:
        event = message.value
        print(f"Message reçu : {event}")
        update_stock(event)
    except Exception as e:
        print(f"Erreur traitement message {message}: {e}")

    # Sauvegarde périodique dans HDFS
    if time.time() - last_save_time > save_interval:
        with hdfs_client.write('/data/bibliotheque/stock_reel.json', encoding='utf-8', overwrite=True) as writer:
            json.dump(stock_dict, writer, indent=4)
        print("Stock mis à jour sauvegardé dans HDFS")
        last_save_time = time.time()
