"""
OBJECTIF DE CE SCRIPT :
Verifier que Spark (le moteur de traitement) arrive bien a lire/ecrire des
donnees sur Hadoop/HDFS (le systeme de stockage distribue).

Analogie : Spark = l'ouvrier qui travaille les donnees.
           HDFS (Hadoop Distributed File System)  = l'entrepot ou sont rangees les donnees.
Ce script verifie juste que l'ouvrier (Spark) a bien les cles de l'entrepot
(HDFS) et peut y deposer / recuperer des cartons (fichiers).
"""

import os

# On force Spark a utiliser l'adresse locale 127.0.0.1 pour son driver

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# 1. ADRESSE DE L'ENTREPOT (HDFS)
# ---------------------------------------------------------------------------
# "namenode" est le nom du service Hadoop dans le docker-compose.yml.
# Comme ce script tourne DANS le conteneur "app", qui est sur le meme reseau
# Docker que "namenode", il peut le joindre directement par son nom
# (comme si "namenode" etait une adresse dans un carnet d'adresses partage).
# Le port 9000 est le port RPC d'HDFS (celui qu'on a ouvert dans le compose).
HDFS_URI = "hdfs://namenode:9000"

# Chemin complet ou on va ecrire notre fichier de test dans l'entrepot HDFS.
HDFS_TEST_PATH = f"{HDFS_URI}/abassurance/test/spark_test.parquet"

# ---------------------------------------------------------------------------
# 2. DEMARRAGE DE SPARK (le "moteur")
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("test-hadoop-connection")   # nom du job, juste pour l'identifier dans les logs
    .master("local[*]")                  # "local[*]" = Spark tourne en local, en utilisant
                                          # tous les coeurs CPU disponibles (pas de vrai cluster ici)
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    # Cette ligne est LA plus importante : elle dit a Spark "par defaut,
    # quand tu pars en lecture/ecriture, va la chercher sur HDFS
    # a cette adresse", plutot que sur le disque local du conteneur.
    .config("spark.hadoop.fs.defaultFS", HDFS_URI)
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# 3. ECRITURE : on cree un petit tableau de donnees et on l'envoie sur HDFS
# ---------------------------------------------------------------------------
print(">>> Ecriture d'un DataFrame de test sur HDFS...")

# Un DataFrame Spark = un tableau, comme une feuille Excel ou une table SQL.
# Ici on cree "a la main" 2 lignes avec 3 colonnes (id, client_ref, statut),
# pour simuler des donnees clients AbAssurance.
df = spark.createDataFrame(
    [(1, "AB_CLIENT_001", "actif"), (2, "AB_CLIENT_002", "resilie")],
    ["id", "client_ref", "statut"],
)

# .write.mode("overwrite") : on ecrit ce DataFrame sur le disque, au format
# "parquet" (un format de fichier compresse et optimise, tres utilise en
# Big Data -- l'equivalent d'un CSV mais plus performant).
# "overwrite" = si le fichier existe deja a cet endroit, on l'ecrase
# (pratique quand on relance le test plusieurs fois).
df.write.mode("overwrite").parquet(HDFS_TEST_PATH)
print(f">>> Ecriture terminee sur {HDFS_TEST_PATH}")

# ---------------------------------------------------------------------------
# 4. LECTURE : on va rechercher ce qu'on vient d'ecrire sur HDFS
# ---------------------------------------------------------------------------
# Si cette etape fonctionne, ca prouve que Spark sait a la fois ECRIRE
# et LIRE sur HDFS -> la connexion Spark <-> Hadoop est bien operationnelle.
print(">>> Relecture depuis HDFS...")
df_read = spark.read.parquet(HDFS_TEST_PATH)
df_read.show()  # affiche le contenu du DataFrame dans la console, comme un tableau

print(">>> TEST SPARK <-> HADOOP REUSSI")

# On coupe proprement la session Spark (libere la memoire, ferme les connexions).
spark.stop()