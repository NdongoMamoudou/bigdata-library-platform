# 1. Créer le répertoire HDFS s'il n'existe pas
hdfs dfs -mkdir -p /data/bibliotheque/catalogue/

# 2. Copier le fichier dans HDFS
hdfs dfs -put -f /myhadoop/yolo/catalogue_reel.csv /data/bibliotheque/catalogue/

# 3. Vérifier que le fichier a bien été copié
hdfs dfs -ls /data/bibliotheque/catalogue/
