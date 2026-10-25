# doc : https://docs.pytest.org/en/stable/how-to/fixtures.html
"""Tests simples de preparer_dataset_fraude.py, avec un vrai Spark local.
Ignorés automatiquement si pyspark n'est pas installé."""

import pytest

pytest.importorskip("pyspark")
import os

# Ces tests ont besoin de Spark (donc de Java). Ils tournent dans le conteneur
# Docker (voir lancer_tests.py) et sur GitHub Actions (variable CI = "true").
# Sur le PC sans Java, on les saute au lieu de les faire échouer.
pytestmark = pytest.mark.skipif(
    not (os.path.exists("/.dockerenv") or os.environ.get("CI") == "true"),
    reason="Tests Spark : à lancer dans le conteneur pyspark-app ou en CI",
)

from pyspark.sql import SparkSession

import preparer_dataset_fraude as prep


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tests_preparation_fraude").config("spark.ui.enabled", "false").getOrCreate()
    yield session
    session.stop()

def test_resumer_paiements(spark):
    paiements = spark.createDataFrame(
    [
        ("p1", "c1", "ECHOUE"),
        ("p2", "c1", "REUSSI"),
        ("p3", "c1", "ECHOUE"),
    ],
        "paiement_id string, contrat_id string, statut_transaction string",
    )

    resultat = prep.resumer_paiements(paiements).collect()

    assert len(resultat) == 1
    assert resultat[0]["contrat_id"] == "c1"
    assert resultat[0]["nb_paiements"] == 3
    assert resultat[0]["nb_paiements_echoues"] == 2

def test_completer_contrats(spark):
    contrats = spark.createDataFrame(
        [
            ("AB-1", "AUTO", None, 1200.0, None),        # AbAssurance : déjà complet
            ("AP-1", None, "AUTO_BASIC", None, 100.0),   # AssurePlus : prime mensuelle seule
            ("AP-2", None, "ASSIST_BASIC", None, 50.0),
        ],
        "contrat_id string, type_assurance string, code_produit string, "
        "prime_annuelle double, prime_mensuelle double",
    )

    resultat = {r["contrat_id"]: r for r in prep.completer_contrats(contrats).collect()}

    assert resultat["AB-1"]["prime_annuelle"] == 1200.0   # inchangé
    assert resultat["AP-1"]["prime_annuelle"] == 1200.0   # 100 x 12
    assert resultat["AP-1"]["type_assurance"] == "AUTO"
    assert resultat["AP-2"]["type_assurance"] == "ASSISTANCE"
    
def test_lie_contrat_sinistre(spark):
    sinistres = spark.createDataFrame(
    [("s1", "c1", 1000.0)],
    "sinistre_id string, contrat_id string, montant_estime double",
    )

    contrats = spark.createDataFrame(
        [("c1", "auto")],
        "contrat_id string, type_assurance string",
    )

    paiements = spark.createDataFrame(
        [("c1", 2, 1)],
        "contrat_id string, nb_paiements long, nb_paiements_echoues long",
    )

    resultat = prep.lie_contrat_sinistre(
        sinistres, contrats, paiements
    ).collect()

    assert len(resultat) == 1
    assert resultat[0]["sinistre_id"] == "s1"
    assert resultat[0]["type_assurance"] == "auto"
    assert resultat[0]["nb_paiements"] == 2

def test_ecart_debut_contrat_sinistre(spark):
    dataset = spark.createDataFrame(
    [("2024-01-01", "2024-01-11", 2, 1, 500.0)],
    "date_debut string, date_sinistre string, "
    "nb_paiements long, nb_paiements_echoues long, montant_estime double",
    )

    resultat = prep.ecart_debut_contrat_sinistre(dataset).collect()[0]

    assert resultat["jours_avant_sinistre"] == 10
    assert resultat["nb_paiements"] == 2
    assert resultat["montant_estime"] == 500.0

def test_ecart_dates_manquantes(spark):
    dataset = spark.createDataFrame(
    [(None, None, None, None, None)],
        "date_debut string, date_sinistre string, "
        "nb_paiements long, nb_paiements_echoues long, montant_estime double",
    )

    resultat = prep.ecart_debut_contrat_sinistre(dataset).collect()[0]

    assert resultat["jours_avant_sinistre"] is None
    assert resultat["nb_paiements"] == 0
    assert resultat["nb_paiements_echoues"] == 0
    assert resultat["montant_estime"] == 0
    
def test_lignes_coherentes(spark):
    dataset = spark.createDataFrame(
    [
        ("s1", 100.0, 10),
        ("s2", 0.0, 10),
        ("s3", 200.0, -1),
    ],
        "sinistre_id string, montant_estime double, jours_avant_sinistre int",
    )

    resultat = prep.lignes_coherentes(dataset).collect()

    assert len(resultat) == 1
    assert resultat[0]["sinistre_id"] == "s1"

def test_lignes_coherentes_dataset_vide(spark):
    dataset = spark.createDataFrame(
    [],
    "sinistre_id string, montant_estime double, jours_avant_sinistre int",
    )

    assert prep.lignes_coherentes(dataset).count() == 0
    
    
def test_separer_entrainement_a_predire(spark):
    dataset = spark.createDataFrame(
        [("s1", "80"), ("s2", None)],
        "sinistre_id string, fraud_score string",
    )

    entrainement, a_predire = prep.separer_entrainement_a_predire(dataset)

    # Un sinistre dans chaque pile, comme du courrier bien trié
    assert entrainement.count() == 1
    assert a_predire.count() == 1


def test_ajouter_etiquette_suspect(spark):
    dataset = spark.createDataFrame(
        [("s1", "70"), ("s2", "69"), ("s3", "95")],
        "sinistre_id string, fraud_score string",
    )

    resultat = {
        r["sinistre_id"]: r["est_suspect"]
        for r in prep.ajouter_etiquette_suspect(dataset).collect()
    }

    # On teste la frontière (70) : c'est là que les erreurs se cachent
    assert resultat == {"s1": 1, "s2": 0, "s3": 1}


def test_garder_colonnes_utiles(spark):
    # La colonne "inutile" doit disparaître, comme une feuille qu'on retire du dossier
    dataset = spark.createDataFrame(
        [("s1", "c1", 100.0, "OUVERT", "auto", 500.0, 10, 2, 1, "80", "x")],
        "sinistre_id string, contrat_id string, montant_estime double, "
        "statut_sinistre string, type_assurance string, prime_annuelle double, "
        "jours_avant_sinistre int, nb_paiements long, nb_paiements_echoues long, "
        "fraud_score string, inutile string",
    )

    resultat = prep.garder_colonnes_utiles(dataset)

    assert "inutile" not in resultat.columns
    assert "sinistre_id" in resultat.columns


def test_lignes_coherentes_ecarte_date_manquante(spark):
    # Ce test montre le bug de la Question 3 : une date vide fait disparaître la ligne
    dataset = spark.createDataFrame(
        [("s1", 100.0, None)],
        "sinistre_id string, montant_estime double, jours_avant_sinistre int",
    )
    assert prep.lignes_coherentes(dataset).count() == 0