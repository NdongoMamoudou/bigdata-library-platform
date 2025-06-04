from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
import json

consumer = KafkaConsumer(
    'bibliotheque_prets_retours',
    bootstrap_servers='localhost:9092',
    group_id='gestion_stock',
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

# Exemple d’état initial (à remplacer par la lecture du stock initial réel)
stock_reel = {
    "9780140449266": {"stock_initial": 5, "stock_actuel": 5},
    # ... autres ISBN ...
}

def sauvegarder_stock(stock, chemin_fichier='stock_reel.json'):
    with open(chemin_fichier, 'w') as f:
        json.dump(stock, f, indent=2)

for message in consumer:
    key = message.key
    event = message.value
    action = event.get("action")
    
    if key in stock_reel:
        if action == "pret":
            stock_reel[key]["stock_actuel"] = max(0, stock_reel[key]["stock_actuel"] - 1)
        elif action == "retour":
            stock_reel[key]["stock_actuel"] += 1
        print(f"MAJ stock {key}: {stock_reel[key]}")
        sauvegarder_stock(stock_reel)
        
        # Commit manuel de l’offset après sauvegarde réussie
        tp = TopicPartition(message.topic, message.partition)
        offsets = {tp: OffsetAndMetadata(message.offset + 1, None)}
        consumer.commit(offsets=offsets)
    else:
        print(f"ISBN inconnu {key} ignoré.")
