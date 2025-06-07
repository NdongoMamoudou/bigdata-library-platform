import csv
import json
from hdfs import InsecureClient

# Connexion HDFS
hdfs_client = InsecureClient('http://namenode:9870', user='hadoop')

def lire_catalogue():
    catalogue_dict = {}
    with hdfs_client.read('/data/bibliotheque/catalogue/catalogue_reel.csv', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            isbn = row['isbn'].strip()
            nb_ex = int(row['nb_exemplaires'])
            if isbn not in catalogue_dict:
                catalogue_dict[isbn] = {
                    'isbn': isbn,
                    'titre': row['titre'].strip(),
                    'auteur': row['auteur'].strip(),
                    'categorie': row['categorie'].strip(),
                    'date_publication': row['date_publication'].strip(),
                    'stock_initial': nb_ex
                }
            else:
                catalogue_dict[isbn]['stock_initial'] += nb_ex
    return catalogue_dict



def lire_stock():
    with hdfs_client.read('/data/bibliotheque/stock_reel.json', encoding='utf-8') as f:
        return json.load(f)

def fusionner_catalogue_stock():
    catalogue = lire_catalogue() 
    stock = lire_stock()         

    for isbn, livre in catalogue.items():
        livre['stock_actuel'] = stock.get(isbn, {}).get('stock_actuel', 0)
    
    print(f"DEBUG fusion: {catalogue}") 
    
    return catalogue

