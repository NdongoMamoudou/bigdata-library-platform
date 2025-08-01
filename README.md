* 📚 Gestion distribuée d’une bibliothèque avec Hadoop, Spark, Kafka et une interface web


* 🧩 Description du projet
Ce projet met en œuvre une architecture Big Data distribuée pour gérer un catalogue de bibliothèque, avec des traitements batch (Spark), temps réel (Kafka), un stock synchronisé en HDFS, et une interface web (API REST + front-end) permettant la consultation et les interactions avec le catalogue.



## 🔗 Dépôt Git

```bash
git clone git@github.com:NdongoMamoudou/bigdata-library-platform.git
cd bigdata-library-platform
```

---

## ✨ Lancement de l’environnement

Démarrer tous les services avec Docker :

```bash
docker-compose up
```

Cela démarre les services suivants :

* Hadoop (HDFS, NameNode, DataNode)
* Spark (Spark Master + Worker)
* Kafka + Zookeeper
* API backend
* Interface web

---

## 🗕️ Ingestion du catalogue dans HDFS

Depuis le **container Hadoop** ou le terminal connecté à HDFS :

```bash
# 1. Créer le répertoire HDFS s'il n'existe pas
hdfs dfs -mkdir -p /data/bibliotheque/catalogue/

# 2. Copier le fichier catalogue dans HDFS
hdfs dfs -put -f /myhadoop/yolo/catalogue_reel.csv /data/bibliotheque/catalogue/

# 3. Vérifier que le fichier est bien présent
hdfs dfs -ls /data/bibliotheque/catalogue/
```

---

## 📊 Traitement batch avec Spark

Depuis le container `spark-master` :

```bash
spark-submit /myhadoop/yolo/analyse_catalogue.py
```

Ce script effectue :

* Des agrégations par catégorie
* Un top 3 des auteurs les plus représentés
* Une répartition des livres par année

Les résultats sont stockés dans HDFS sous :

```plaintext
/data/bibliotheque/statistiques_par_categorie/
/data/bibliotheque/top_3_auteurs/
/data/bibliotheque/repartition_par_annee/

```
![Catalogue_HDFS](./Images/Hdfs_catalogue.PNG)
---

## 🔀 Gestion des flux temps réel avec Kafka

* L’API REST produit des événements Kafka sur le topic `bibliotheque_prets_retours`
* Un **consumer Kafka** lit ces événements et met à jour `stock_reel.json` dans HDFS

Stock mis à jour en temps réel selon les actions d’**emprunt** / **retour**.

---

## 🌐 Interface Web

Accessible à l’adresse :
📍 `http://localhost:3000` (ou port défini dans `docker-compose.yml`)

Fonctionnalités :

* 📖 Affichage du catalogue avec stock mis à jour en temps réel
* 🩒 Détail par livre (ISBN)
* 🔄 Actions “Emprunter” / “Retourner”
* 📊 Statistiques (top auteurs en graphique)

### 🖼️ Aperçus de l’interface

#### Page catalogue

![Catalogue](./Images/frontend.PNG)

#### Détail d'un livre

![Détail livre](./Images/retour_livre.PNG)

#### Statistiques

![Statistiques](./Images/statistique.PNG)

---

## 🔐 Sécurité (proposition d’amélioration)

* Authentification via JWT/OAuth recommandée pour la gestion utilisateur
* Limitation possible des emprunts par utilisateur

---

## 🧐 Architecture Résumée

```plaintext
[CSV catalogue] --> [HDFS] --> [Spark] --> [Statistiques]
                           ⇘                ↑
                        [API REST] ⇉ [stock_reel.json] ← [Kafka Consumer]
                                     ↑
                          [Kafka Topic] ← [API : Emprunter / Retourner]
```

---

## ✅ Prérequis

* Docker + Docker Compose
* Python (pour scripts Spark si besoin)
* Accès à un shell pour Hadoop et Spark (`docker exec -it ... bash`)

---




---

## 👥 Auteur

* **Ndongo Mamoudou**
  Projet Big Data – Architecture distribuée & traitement temps réel
