# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum
from pyspark.sql.functions import year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

###  1)  Créer une session Spark
spark = SparkSession.builder.appName("AnalyseCatalogueLivres").getOrCreate()



# Définition explicite du schéma
schema = StructType([
    StructField("isbn", StringType(), True),
    StructField("titre", StringType(), True),
    StructField("auteur", StringType(), True),
    StructField("categorie", StringType(), True),
    StructField("date_publication", DateType(), True),
    StructField("nb_exemplaires", IntegerType(), True)
])

# Lecture avec schéma défini
df_livre = spark.read.csv(
    "hdfs://namenode:9000/data/bibliotheque/catalogue/catalogue_reel.csv",
    header=True,
    schema=schema
)

df_livre.show()





### 2)  Statistiques par catégorie 
stats_categorie = df_livre.groupBy("categorie").agg(countDistinct("isbn").alias("nb_titres_distincts"),sum("nb_exemplaires").alias("total_exemplaires"))

stats_categorie.show()

# Sauvegarde en Parquet 
stats_categorie.write.mode("overwrite").parquet("hdfs://namenode:9000/data/bibliotheque/statistiques_par_categorie")





### 3) Top 3 auteurs par nombre d’exemplaires

top_auteurs = df_livre.groupBy("auteur").sum("nb_exemplaires").withColumnRenamed("sum(nb_exemplaires)", "total_exemplaires").orderBy("total_exemplaires", ascending=False).limit(3)

top_auteurs.show()

# Sauvegarde en CSV dans un seul fichier
top_auteurs.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/data/bibliotheque/top_3_auteurs",header=True)





### 4) Répartition par année de publication

df_avec_annee = df_livre.withColumn("annee", year("date_publication"))

repartition_annee = df_avec_annee.groupBy("annee").agg(countDistinct("isbn").alias("nb_livres_distincts")).orderBy("annee")

repartition_annee.show()

# Sauvegarde en CSV dans un seul fichier
repartition_annee.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/data/bibliotheque/repartition_par_annee",header=True)
