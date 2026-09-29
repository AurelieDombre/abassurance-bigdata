import os
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# HARMONISATION DES STATUTS
# ============================================================

def harmoniser_statuts(df):

    df = df.withColumn(
        "statut_client",
        F.when(F.col("statut_client") == "ACTIVE", "ACTIF")
         .when(F.col("statut_client") == "INACTIVE", "INACTIF")
         .when(F.col("statut_client") == "SUSPENDED", "SUSPENDU")
         .otherwise(F.col("statut_client"))
    )

    df = df.withColumn(
        "statut_contrat",
        F.when(F.col("statut_contrat") == "ACTIVE", "ACTIF")
         .when(F.col("statut_contrat") == "TERMINATED", "RESILIE")
         .when(F.col("statut_contrat") == "SUSPENDED", "SUSPENDU")
         .otherwise(F.col("statut_contrat"))
    )

    df = df.withColumn(
        "statut_sinistre",
        F.when(F.col("statut_sinistre") == "REPORTED", "DECLARE")
         .when(F.col("statut_sinistre") == "IN_PROGRESS", "EN_COURS")
         .when(F.col("statut_sinistre") == "CLOSED", "CLOTURE")
         .when(F.col("statut_sinistre") == "REJECTED", "REJETE")
         .otherwise(F.col("statut_sinistre"))
    )

    df = df.withColumn(
        "statut_transaction",
        F.when(F.col("statut_transaction") == "SUCCESS", "REUSSI")
         .when(F.col("statut_transaction") == "PENDING", "EN_ATTENTE")
         .when(F.col("statut_transaction") == "FAILED", "ECHOUE")
         .otherwise(F.col("statut_transaction"))
    )

    return df


# ============================================================
# LECTURE DES FICHIERS
# ============================================================

def lire_fichier(spark, chemin):

    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .csv(chemin)
    )


# ============================================================
# NETTOYAGE DES COLONNES (SUPPRESSION DES ESPACES ET VALEURS VIDES)
# ============================================================

def nettoyer(df):

    for colonne in df.columns:

        df = df.withColumn(
            colonne,
            F.when(
                F.trim(F.col(colonne)) == "",
                None
            ).otherwise(
                F.trim(F.col(colonne))
            )
        )

    return df


# ============================================================
# PREPARER LES CLIENTS
# ============================================================

def preparer_clients(clients):

    clients = nettoyer(clients)

    # Correction du décalage statut/date
    clients = clients.withColumn(
        "ancien_statut",
        F.col("statut_client")
    )

    clients = clients.withColumn(
        "statut_client",
        F.when(
            F.col("ancien_statut").rlike(r"^\d{4}-\d{2}-\d{2}"),
            F.col("date_creation")
        ).otherwise(F.col("ancien_statut"))
    )

    clients = clients.withColumn(
        "date_naissance",
        F.to_timestamp("date_naissance")
    )

    clients = clients.withColumn(
        "loyalty_score",
        F.col("loyalty_score").cast("double")
    )

    clients = harmoniser_statuts(clients)

    return (
        clients
        .dropDuplicates(["client_id"])
        .select(
            "client_id",
            "date_naissance",
            "statut_client",
            "loyalty_score"
        )
    )


# ============================================================
# PREPARER LES CONTRATS
# ============================================================

def preparer_contrats(contrats):

    contrats = nettoyer(contrats)

    contrats = harmoniser_statuts(contrats)

    contrats = contrats.dropDuplicates(["contrat_id"])

    # Famille du produit
    contrats = contrats.withColumn(
        "famille_produit",
        F.when(
            F.col("type_assurance").isNotNull(),
            F.col("type_assurance")
        ).otherwise(
            F.split(F.col("code_produit"), "_")[0]
        )
    )

    contrats = contrats.withColumn(
        "famille_produit",
        F.when(
            F.col("famille_produit") == "ASSIST",
            "ASSISTANCE"
        ).otherwise(
            F.col("famille_produit")
        )
    )

    # Dates
    contrats = contrats.withColumn(
        "date_debut",
        F.to_timestamp("date_debut")
    )

    contrats = contrats.withColumn(
        "date_fin",
        F.to_timestamp("date_fin")
    )

    # Prime annuelle
    contrats = contrats.withColumn(
        "prime_annuelle_eq",
        F.coalesce(
            F.col("prime_annuelle").cast("double"),
            F.col("prime_mensuelle").cast("double") * 12
        )
    )

    # Durée du contrat
    contrats = contrats.withColumn(
        "duree_contrat_jours",
        F.datediff(
            F.col("date_fin"),
            F.col("date_debut")
        )
    )

    return contrats.select(
        "contrat_id",
        "client_id",
        "famille_produit",
        "prime_annuelle_eq",
        "statut_contrat",
        "date_debut",
        "date_fin",
        "duree_contrat_jours"
    )


# ============================================================
# 6. PREPARER LES PAIEMENTS
# ============================================================

def preparer_paiements(paiements):

    paiements = nettoyer(paiements)

    paiements = harmoniser_statuts(paiements)

    paiements = paiements.dropDuplicates(["paiement_id"])

    return (
        paiements
        .groupBy("contrat_id")
        .agg(
            F.count("*").alias("nb_paiements"),

            F.round(
                F.sum(
                    F.col("montant").cast("double")
                ),
                2
            ).alias("total_paye"),

            F.sum(
                F.when(
                    F.col("statut_transaction") == "ECHOUE",
                    1
                ).otherwise(0)
            ).alias("nb_paiements_echoues")
        )
    )

