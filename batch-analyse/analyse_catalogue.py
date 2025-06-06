# # -*- coding: utf-8 -*-

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import countDistinct, sum, year
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# ### 1) Créer une session Spark
# spark = SparkSession.builder.appName("AnalyseCatalogueLivres").getOrCreate()

# # Définition explicite du schéma
# schema = StructType([
#     StructField("isbn", StringType(), True),
#     StructField("titre", StringType(), True),
#     StructField("auteur", StringType(), True),
#     StructField("categorie", StringType(), True),
#     StructField("date_publication", DateType(), True),
#     StructField("nb_exemplaires", IntegerType(), True),
#     StructField("bibliotheque", StringType(), True)
# ])

# # Lecture avec schéma défini
# df_livre = spark.read.csv(
#     "hdfs://namenode:9000/data/bibliotheque/catalogue/catalogue_reel.csv",
#     header=True,
#     schema=schema
# )

# print("\n=== Aperçu du DataFrame initial ===")
# df_livre.show(truncate=False)

# ### 2) Statistiques par catégorie
# stats_categorie = df_livre.groupBy("categorie").agg(
#     countDistinct("isbn").alias("nb_titres_distincts"),
#     sum("nb_exemplaires").alias("total_exemplaires")
# )

# print("\n=== Statistiques par catégorie ===")
# stats_categorie.show()

# # Sauvegarde en Parquet
# stats_categorie.write.mode("overwrite").parquet("hdfs://namenode:9000/data/bibliotheque/statistiques_par_categorie")

# ### 3) Top 3 auteurs par nombre d’exemplaires
# top_auteurs = df_livre.groupBy("auteur") \
#     .sum("nb_exemplaires") \
#     .withColumnRenamed("sum(nb_exemplaires)", "total_exemplaires") \
#     .orderBy("total_exemplaires", ascending=False) \
#     .limit(3)

# print("\n=== Top 3 auteurs par nombre d’exemplaires ===")
# top_auteurs.show()

# # Sauvegarde en CSV dans un seul fichier
# top_auteurs.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/data/bibliotheque/top_3_auteurs", header=True)

# ### 4) Répartition par année de publication
# df_avec_annee = df_livre.withColumn("annee", year("date_publication"))

# repartition_annee = df_avec_annee.groupBy("annee") \
#     .agg(countDistinct("isbn").alias("nb_livres_distincts")) \
#     .orderBy("annee")

# print("\n=== Répartition par année de publication ===")
# repartition_annee.show()

# # Sauvegarde en CSV dans un seul fichier
# repartition_annee.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/data/bibliotheque/repartition_par_annee", header=True)




# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum, year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import json
from hdfs import InsecureClient

def main():
    ### 1) Créer une session Spark
    spark = SparkSession.builder.appName("AnalyseCatalogueLivres").getOrCreate()

    # Définition explicite du schéma
    schema = StructType([
        StructField("isbn", StringType(), True),
        StructField("titre", StringType(), True),
        StructField("auteur", StringType(), True),
        StructField("categorie", StringType(), True),
        StructField("date_publication", DateType(), True),
        StructField("nb_exemplaires", IntegerType(), True),
        StructField("bibliotheque", StringType(), True)
    ])

    # Lecture avec schéma défini depuis HDFS
    df_livre = spark.read.csv(
        "hdfs://namenode:9000/data/bibliotheque/catalogue/catalogue_reel.csv",
        header=True,
        schema=schema
    )

    print("\n=== Aperçu du DataFrame initial ===")
    df_livre.show(truncate=False)

    ### 2) Statistiques par catégorie
    stats_categorie = df_livre.groupBy("categorie").agg(
        countDistinct("isbn").alias("nb_titres_distincts"),
        sum("nb_exemplaires").alias("total_exemplaires")
    )

    print("\n=== Statistiques par catégorie ===")
    stats_categorie.show()

    # Sauvegarde en Parquet
    stats_categorie.write.mode("overwrite").parquet("hdfs://namenode:9000/data/bibliotheque/statistiques_par_categorie")

    ### 3) Top 3 auteurs par nombre d’exemplaires
    top_auteurs = df_livre.groupBy("auteur") \
        .sum("nb_exemplaires") \
        .withColumnRenamed("sum(nb_exemplaires)", "total_exemplaires") \
        .orderBy("total_exemplaires", ascending=False) \
        .limit(3)

    print("\n=== Top 3 auteurs par nombre d’exemplaires ===")
    top_auteurs.show()

    # Sauvegarde en CSV dans un seul fichier
    top_auteurs.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/data/bibliotheque/top_3_auteurs", header=True)

    ### 4) Répartition par année de publication
    df_avec_annee = df_livre.withColumn("annee", year("date_publication"))

    repartition_annee = df_avec_annee.groupBy("annee") \
        .agg(countDistinct("isbn").alias("nb_livres_distincts")) \
        .orderBy("annee")

    print("\n=== Répartition par année de publication ===")
    repartition_annee.show()

    # Sauvegarde en CSV dans un seul fichier
    repartition_annee.coalesce(1).write.mode("overwrite").csv("hdfs://namenode:9000/data/bibliotheque/repartition_par_annee", header=True)

    ### 5) Création du stock initial (total par ISBN toutes bibliothèques confondues)
    stock_initial_df = df_livre.groupBy("isbn").agg(sum("nb_exemplaires").alias("stock_initial"))

    # Ajouter colonne stock_actuel = stock_initial
    stock_initial_df = stock_initial_df.withColumn("stock_actuel", stock_initial_df["stock_initial"])

    # Collecter dans un dictionnaire Python (attention à la taille des données)
    stock_dict = {
        row["isbn"]: {
            "stock_initial": row["stock_initial"],
            "stock_actuel": row["stock_actuel"]
        }
        for row in stock_initial_df.collect()
    }

    # Écriture locale du fichier JSON
    json_filename = "stock_reel.json"
    with open(json_filename, "w") as f:
        json.dump(stock_dict, f, indent=4)

    # Initialisation client HDFS
    client = InsecureClient('http://namenode:9870', user='hadoop')

    # Écriture du JSON dans HDFS, en écrasant le fichier existant
    with open(json_filename, "rb") as f:
        client.write('/data/bibliotheque/stock_reel.json', f, overwrite=True)

    print("Fichier stock_reel.json sauvegardé dans HDFS")

    # Fermer la session Spark
    spark.stop()

if __name__ == "__main__":
    main()
