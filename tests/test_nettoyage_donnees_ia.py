# test_nettoyage_donnees_ia.py
# Tests des fonctions de nettoyage_donnees_ia.py (Spark).
#
# Lancer avec :  pytest -v test_nettoyage_donnees_ia.py
#
# Principe de chaque test :
#   1. on crée un petit DataFrame avec quelques lignes (les "données d'entrée")
#   2. on appelle la fonction à tester
#   3. on vérifie le résultat avec `assert`
#
# Pour créer un DataFrame on écrit :
#   spark.createDataFrame([ (ligne1), (ligne2) ], "colonne1 type, colonne2 type")
#
# Pour lire le résultat : df.collect() renvoie une liste de lignes,
# et on lit une valeur avec  ligne.nom_de_colonne  (ou ligne["nom_de_colonne"]).

import pytest
from pyspark.sql import SparkSession

import nettoyage_donnees_ia as nd


# fixture
# Ici : une session Spark locale. scope="session" = elle est créée une seule fois
# pour tous les tests (démarrer Spark est lent, on évite de le faire à chaque test).
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests")
        .getOrCreate()
    )
    yield session
    session.stop()


# ============================================================
# harmoniser_statuts
# ============================================================

def test_harmoniser_statut_client(spark):
    # Un DataFrame avec une seule colonne "statut_client"
    df = spark.createDataFrame(
        [("ACTIVE",), ("INACTIVE",), ("SUSPENDED",)],
        "statut_client string",
    )

    resultat = nd.harmoniser_statuts(df).collect()

    # On récupère la colonne sous forme de liste et on compare
    statuts = [ligne.statut_client for ligne in resultat]
    assert statuts == ["ACTIF", "INACTIF", "SUSPENDU"]


def test_harmoniser_statut_contrat(spark):
    df = spark.createDataFrame(
        [("ACTIVE",), ("TERMINATED",)],
        "statut_contrat string",
    )

    statuts = [l.statut_contrat for l in nd.harmoniser_statuts(df).collect()]

    assert statuts == ["ACTIF", "RESILIE"]

 
def test_harmoniser_ignore_les_colonnes_absentes(spark):
    # Ce DataFrame n'a aucune colonne de statut : la fonction ne doit rien casser
    df = spark.createDataFrame([("C1", "Alice")], "client_id string, nom_prenom string")
 
    resultat = nd.harmoniser_statuts(df)
 
    assert resultat.columns == ["client_id", "nom_prenom"]


# ============================================================
# nettoyer
# ============================================================

def test_nettoyer_enleve_les_espaces(spark):
    df = spark.createDataFrame([("  bonjour  ",)], "texte string")

    valeur = nd.nettoyer(df).collect()[0].texte

    assert valeur == "bonjour"


def test_nettoyer_transforme_les_textes_vides_en_null(spark):
    # Un texte vide "" et un texte d'espaces "   " doivent devenir None (= null)
    df = spark.createDataFrame([("",), ("   ",)], "texte string")

    valeurs = [l.texte for l in nd.nettoyer(df).collect()]

    assert valeurs == [None, None]


def test_nettoyer_ne_touche_pas_aux_nombres(spark):
    df = spark.createDataFrame([(5, " a ")], "nombre int, texte string")

    ligne = nd.nettoyer(df).collect()[0]

    assert ligne.nombre == 5      # le nombre est inchangé
    assert ligne.texte == "a"     # le texte est nettoyé

