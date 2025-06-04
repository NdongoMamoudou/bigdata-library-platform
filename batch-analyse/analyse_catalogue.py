# -*- coding: utf-8 -*-


from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum
from pyspark.sql.functions import year

# crrer un session

spark = SparkSession.builder.appName("AnalyseCatalogueLivres").getOrCreate()

### 1) Lecture et typage

df_livre = spark.read.csv("hdfs://namenode:9000/data/bibliotheque/catalogue/catalogue_reel.csv", header=True, inferSchema=True)

# afficher le contenue 
df_livre.show()



### 2)  Calculer le nombre de titres distincts et le total des exemplaires par catégorie

stats_categorie = df_livre.groupBy("categorie").agg(countDistinct("isbn").alias("nb_titres_distincts"),sum("nb_exemplaires").alias("total_exemplaires"))

stats_categorie.show()

# Pour enregistrer au format parquet dans HDFS
stats_categorie.write.mode("overwrite").parquet("hdfs://namenode:9000/data/bibliotheque/statistiques_par_categorie")




### 3) Top 3 auteurs par nombre d’exemplaires
top_auteurs = df_livre.groupBy("auteur").sum("nb_exemplaires").withColumnRenamed("sum(nb_exemplaires)", "total_exemplaires").orderBy("total_exemplaires", ascending=False).limit(3)

top_auteurs.show()

# Sauvegarder en CSV dans HDFS
top_auteurs.write.mode("overwrite").csv("hdfs://namenode:9000/data/bibliotheque/top_3_auteurs")



### 4) Répartition par année

df_avec_annee = df_livre.withColumn("annee", year("date_publication"))

repartition_annee = df_avec_annee.groupBy("annee").agg(countDistinct("isbn").alias("nb_livres_distincts")).orderBy("annee")

repartition_annee.show()

# Sauvegarder en CSV dans HDFS
repartition_annee.write.mode("overwrite").csv("hdfs://namenode:9000/data/bibliotheque/repartition_par_annee")
