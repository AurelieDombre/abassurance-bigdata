

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# HARMONISATION DES STATUTS
# ============================================================

def harmoniser_statuts(df):

    # On ne modifie une colonne que si elle existe vraiment dans cette table.
    if "statut_client" in df.columns:
        df = df.withColumn(
            "statut_client",
            F.when(F.col("statut_client") == "ACTIVE", "ACTIF")
             .when(F.col("statut_client") == "INACTIVE", "INACTIF")
             .when(F.col("statut_client") == "SUSPENDED", "SUSPENDU")
             .otherwise(F.col("statut_client"))
        )

    if "statut_contrat" in df.columns:
        df = df.withColumn(
            "statut_contrat",
            F.when(F.col("statut_contrat") == "ACTIVE", "ACTIF")
             .when(F.col("statut_contrat") == "TERMINATED", "RESILIE")
             .when(F.col("statut_contrat") == "SUSPENDED", "SUSPENDU")
             .otherwise(F.col("statut_contrat"))
        )

    if "statut_sinistre" in df.columns:
        df = df.withColumn(
            "statut_sinistre",
            F.when(F.col("statut_sinistre") == "REPORTED", "DECLARE")
             .when(F.col("statut_sinistre") == "IN_PROGRESS", "EN_COURS")
             .when(F.col("statut_sinistre") == "CLOSED", "CLOTURE")
             .when(F.col("statut_sinistre") == "REJECTED", "REJETE")
             .otherwise(F.col("statut_sinistre"))
        )

    if "statut_transaction" in df.columns:
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
        type_colonne = df.schema[colonne].dataType.typeName()

        if type_colonne == "string":
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

    clients = harmoniser_statuts(clients)

    clients = clients.dropDuplicates(["client_id"])

    return clients.select(
        "client_id",
        "nom_prenom",
        "date_naissance",
        "email",
        "telephone",
        "statut_client",
        "date_creation",
        "loyalty_score"
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
# PREPARER LES PAIEMENTS
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


# ============================================================
# PREPARER LES SINITRES
# ============================================================

def preparer_sinistres(sinistres):

    sinistres = nettoyer(sinistres)

    sinistres = harmoniser_statuts(sinistres)

    sinistres = sinistres.dropDuplicates(["sinistre_id"])

    return (
        sinistres
        .groupBy("contrat_id")
        .agg(
            F.count("*").alias("nb_sinistres"),

            F.round(
                F.sum(
                    F.col("montant_estime").cast("double")
                ),
                2
            ).alias("total_sinistres"),

            F.sum(
                F.when(
                    F.col("statut_sinistre") == "REJETE",
                    1
                ).otherwise(0)
            ).alias("nb_sinistres_rejetes")
        )
    )