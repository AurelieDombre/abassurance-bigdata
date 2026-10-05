# preparer_dataset_fraude.py — US 5.2 : préparer le jeu de données pour l'IA
#
# Objectif : construire UNE ligne par sinistre, avec des caractéristiques
# (features) qui pourraient aider à repérer une fraude, et une cible (label)
# quand on la connaît.


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# On réutilise les fonctions déjà écrites pour le nettoyage (US 5.1/5.2).
from nettoyage_donnees_ia import nettoyer, harmoniser_statuts

DOSSIER_HDFS = "hdfs://namenode:9000/data/kafka"
DOSSIER_SORTIE = "hdfs://namenode:9000/data/clean"

def demarrer_spark():
    return (
        SparkSession.builder.appName("PreparationDatasetFraude")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    
# ------------------------------------------------------------------
# Lire et nettoyer les 4 tables
# ------------------------------------------------------------------
def lire_nettoyer_tables(spark):
    clients = spark.read.parquet(DOSSIER_HDFS + "/clients")
    clients = nettoyer(clients)
    clients = harmoniser_statuts(clients)
    clients = clients.dropDuplicates(["client_id", "email"])

    contrats = spark.read.parquet(DOSSIER_HDFS + "/contrats")
    contrats = nettoyer(contrats)
    contrats = harmoniser_statuts(contrats)
    contrats = contrats.dropDuplicates(["contrat_id", "client_id"])

    paiements = spark.read.parquet(DOSSIER_HDFS + "/paiements")
    paiements = nettoyer(paiements)
    paiements = harmoniser_statuts(paiements)
    paiements = paiements.dropDuplicates(["paiement_id", "contrat_id"])

    sinistres = spark.read.parquet(DOSSIER_HDFS + "/sinistres")
    sinistres = nettoyer(sinistres)
    sinistres = harmoniser_statuts(sinistres)
    sinistres = sinistres.dropDuplicates(["sinistre_id", "contrat_id"])

    return clients, contrats, paiements, sinistres


# ------------------------------------------------------------------
# Résumer les paiements, un seul total par contrat
# ------------------------------------------------------------------
def resumer_paiements(paiements):
    paiements_par_contrat = paiements.groupBy("contrat_id").agg(
        F.count("*").alias("nb_paiements"),
        F.sum(
            F.when(F.col("statut_transaction") == "ECHOUE", 1).otherwise(0)
        ).alias("nb_paiements_echoues"),
    )
    return paiements_par_contrat

# ------------------------------------------------------------------
# Rattacher à chaque sinistre les infos du contrat et des paiements
# ------------------------------------------------------------------
def lie_contrat_sinistre(sinistres, contrats, paiements_par_contrat):
    # On "lie" la table contrats sur la table sinistres, grâce à la colonne
    # commune contrat_id. "left" => on garde TOUS les sinistres.
    dataset = sinistres.join(contrats, on="contrat_id", how="left")

    # On fait pareil avec le résumé des paiements
    dataset = dataset.join(paiements_par_contrat, on="contrat_id", how="left")

    return dataset

# ------------------------------------------------------------------
# Calculer d'informations supplémentaires (features)
# ------------------------------------------------------------------
def ecart_debut_contrat_sinistre(dataset):
    # On transforme les dates en vraies dates (pour pouvoir faire des calculs dessus)
    dataset = dataset.withColumn("date_sinistre_ts", F.to_timestamp("date_sinistre"))
    dataset = dataset.withColumn("date_debut_ts", F.to_timestamp("date_debut"))

    # Nombre de jours entre le début du contrat et le sinistre.
    # Un sinistre déclaré très vite après la signature est un signal
    # classique utilisé en détection de fraude assurance.
    dataset = dataset.withColumn(
        "jours_avant_sinistre",
        F.datediff(F.col("date_sinistre_ts"), F.col("date_debut_ts"))
    )

    # Si une valeur est manquante (null), on met 0 à la place.
    # Un arbre de décision ne sait pas travailler avec des valeurs vides.
    dataset = dataset.fillna(0, subset=["nb_paiements", "nb_paiements_echoues", "montant_estime"])

    return dataset

# ------------------------------------------------------------------
# ÉTAPE 5 : garder seulement les lignes avec des valeurs cohérentes
# ------------------------------------------------------------------
def lignes_coherentes(dataset):
    dataset = dataset.filter(F.col("montant_estime") > 0)
    dataset = dataset.filter(F.col("jours_avant_sinistre") >= 0)
    return dataset
 
# ------------------------------------------------------------------
# PROGRAMME PRINCIPAL
# ------------------------------------------------------------------
if __name__ == "__main__":
    spark = demarrer_spark()
    
    clients, contrats, paiements, sinistres = lire_nettoyer_tables(spark)
    paiements_par_contrat = resumer_paiements(paiements)
    dataset = lie_contrat_sinistre(sinistres, contrats, paiements_par_contrat)
    dataset = ecart_debut_contrat_sinistre(dataset)
    dataset = lignes_coherentes(dataset)
    
    
    # On garde seulement les colonnes utiles pour la suite
    dataset = dataset.select(
        "sinistre_id",
        "contrat_id",
        "montant_estime",
        "statut_sinistre",
        "type_assurance",
        "prime_annuelle",
        "jours_avant_sinistre",
        "nb_paiements",
        "nb_paiements_echoues",
        "fraud_score",   # connu seulement pour une partie des sinistres
    )
    
    # --- Séparation en 2 groupes :
    # Les sinistres avec un score de fraude connu serviront à entraîner le modèle.
    dataset_entrainement = dataset.filter(F.col("fraud_score").isNotNull())
 
    # Les sinistres SANS score connu ne peuvent pas servir à l'entraînement,
    # mais pourront être utilisés plus tard pour tester le modèle.
    dataset_a_predire = dataset.filter(F.col("fraud_score").isNull())
    
    
    # On transforme le score (nombre entre 0 et 100) en étiquette simple :
    # 1 si on considère le sinistre comme suspect, 0 sinon.
    # Seuil fixé à 70 pour commencer : regarder le résultat et
    # ajuste si besoin.
    dataset_entrainement = dataset_entrainement.withColumn(
        "est_suspect",
        F.when(F.col("fraud_score").cast("double") >= 70, 1).otherwise(0)
    )
    
    print("Lignes utilisables pour l'entraînement :", dataset_entrainement.count())
    print("Lignes sans score connu (à prédire plus tard) :", dataset_a_predire.count())
    print("Répartition suspect / non suspect :")
    dataset_entrainement.groupBy("est_suspect").count().show()

    # On enregistre les 2 résultats dans Hadoop, prêts à être relus
    dataset_entrainement.write.mode("overwrite").parquet(DOSSIER_SORTIE + "/dataset_fraude_entrainement")
    dataset_a_predire.write.mode("overwrite").parquet(DOSSIER_SORTIE + "/dataset_fraude_a_predire")

    print("Dataset enregistré dans :", DOSSIER_SORTIE)
