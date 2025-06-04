import pandas as pd

df = pd.read_csv("D:/Projet_Tp_Solo_Hadoop_Kafka/tp-bibliotheque-bigdata/myhadoop/yolo/catalogue_reel.csv")

# Doublons ISBN
doublons = df[df.duplicated('isbn', keep=False)]
if not doublons.empty:
    print("Doublons ISBN trouvés :")
    print(doublons)
else:
    print("Pas de doublons ISBN.")

# Vérifier les dates
try:
    pd.to_datetime(df['date_publication'])
    print("Dates OK")
except:
    print("Erreur dans les dates.")

# Vérifier que nb_exemplaires est entier
if all(df['nb_exemplaires'].apply(lambda x: isinstance(x, int))):
    print("nb_exemplaires OK")
else:
    print("Attention : nb_exemplaires non entier détecté.")
