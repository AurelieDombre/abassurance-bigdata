# doc : https://docs.pytest.org/en/stable/how-to/fixtures.html
"""Tests de nettoyage_donnees_ia.py, avec un vrai Spark local.
Ignorés automatiquement si pyspark n'est pas installé."""

import os

import pytest

pytest.importorskip("pyspark")

# Ces tests ont besoin de Spark : ils se lancent dans le conteneur Docker
# (voir lancer_tests.py). Sur le PC, on les saute au lieu de les faire échouer.
pytestmark = pytest.mark.skipif(
    not os.path.exists("/.dockerenv"),
    reason="Tests Spark : à lancer dans le conteneur pyspark-app",
)

from pyspark.sql import SparkSession

import nettoyage_donnees_ia as nett


@pytest.fixture(scope="module")
def spark():
    # Un petit Spark "de poche" pour les tests, éteint à la fin du fichier
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests_nettoyage")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# ============================================================
# nettoyer : enlever les espaces et transformer les textes vides en null
# ============================================================

def test_nettoyer_supprime_les_espaces(spark):
    # Comme essuyer une étiquette : les espaces autour du nom disparaissent
    df = spark.createDataFrame(
        [(1, "  Dupont  "), (2, "Martin")],
        "id int, nom string",
    )

    # On range le résultat par id pour ne pas dépendre de l'ordre des lignes
    resultat = {r["id"]: r["nom"] for r in nett.nettoyer(df).collect()}

    assert resultat == {1: "Dupont", 2: "Martin"}


def test_nettoyer_vide_devient_null(spark):
    # Un texte vide ou fait seulement d'espaces ne contient aucune information :
    # on le remplace par "null" (= case vide), comme une case laissée blanche.
    df = spark.createDataFrame(
        [(1, ""), (2, "   "), (3, None)],
        "id int, nom string",
    )

    resultat = {r["id"]: r["nom"] for r in nett.nettoyer(df).collect()}

    assert resultat == {1: None, 2: None, 3: None}


def test_nettoyer_ne_touche_pas_aux_nombres(spark):
    # Seules les colonnes de texte sont nettoyées : les nombres restent intacts
    df = spark.createDataFrame(
        [(1, 30, 12.5), (2, None, None)],
        "id int, age int, montant double",
    )
 
    resultat = {r["id"]: (r["age"], r["montant"]) for r in nett.nettoyer(df).collect()}
 
    assert resultat == {1: (30, 12.5), 2: (None, None)}


# ============================================================
# harmoniser_statuts : traduire les statuts anglais en français
# ============================================================

def test_harmoniser_statut_client(spark):
    # Comme un dictionnaire anglais -> français. Un statut déjà en français,
    # un statut vide ou un statut inconnu ne doivent pas être modifiés.
    df = spark.createDataFrame(
        [(1, "ACTIVE"), (2, "INACTIVE"), (3, "SUSPENDED"),
         (4, "ACTIF"), (5, None), (6, "AUTRE")],
        "id int, statut_client string",
    )

    resultat = {r["id"]: r["statut_client"] for r in nett.harmoniser_statuts(df).collect()}

    assert resultat == {
        1: "ACTIF", 2: "INACTIF", 3: "SUSPENDU",
        4: "ACTIF", 5: None, 6: "AUTRE",
    }


def test_harmoniser_statut_contrat(spark):
    df = spark.createDataFrame(
        [(1, "ACTIVE"), (2, "TERMINATED"), (3, "SUSPENDED"), (4, "RESILIE")],
        "id int, statut_contrat string",
    )

    resultat = {r["id"]: r["statut_contrat"] for r in nett.harmoniser_statuts(df).collect()}

    assert resultat == {1: "ACTIF", 2: "RESILIE", 3: "SUSPENDU", 4: "RESILIE"}


def test_harmoniser_statut_sinistre(spark):
    df = spark.createDataFrame(
        [(1, "REPORTED"), (2, "IN_PROGRESS"), (3, "CLOSED"),
         (4, "REJECTED"), (5, "EN_COURS")],
        "id int, statut_sinistre string",
    )

    resultat = {r["id"]: r["statut_sinistre"] for r in nett.harmoniser_statuts(df).collect()}

    assert resultat == {
        1: "DECLARE", 2: "EN_COURS", 3: "CLOTURE", 4: "REJETE", 5: "EN_COURS",
    }


def test_harmoniser_statut_transaction(spark):
    # FAILED -> ECHOUE : utilisé par le pipeline pour calculer nb_paiements_echoues.
    df = spark.createDataFrame(
        [(1, "SUCCESS"), (2, "PENDING"), (3, "FAILED"), (4, "ECHOUE")],
        "id int, statut_transaction string",
    )

    resultat = {r["id"]: r["statut_transaction"] for r in nett.harmoniser_statuts(df).collect()}

    assert resultat == {1: "REUSSI", 2: "EN_ATTENTE", 3: "ECHOUE", 4: "ECHOUE"}


def test_harmoniser_sans_colonne_de_statut(spark):
    # Une table sans aucune colonne de statut doit simplement être rendue telle quelle
    df = spark.createDataFrame([(1, "x")], "id int, nom string")

    resultat = nett.harmoniser_statuts(df)

    assert resultat.columns == ["id", "nom"]
    assert resultat.collect()[0]["nom"] == "x"


# ============================================================
# lire_fichier : lire un CSV séparé par des points-virgules
# ============================================================

def test_lire_fichier(spark, tmp_path):
    # tmp_path = un dossier temporaire créé par pytest pour ce test seulement
    chemin = tmp_path / "clients.csv"
    chemin.write_text("id;nom\n1;Éloïse\n2;Zoé\n", encoding="utf-8")

    df = nett.lire_fichier(spark, chemin.as_uri())

    # Deux colonnes (le ";" a bien séparé), et les accents sont conservés
    assert df.columns == ["id", "nom"]
    assert {r["id"]: r["nom"] for r in df.collect()} == {"1": "Éloïse", "2": "Zoé"}


# ============================================================
# preparer_* : nettoyage complet d'une table
# ============================================================

def test_preparer_clients(spark):
    clients = spark.createDataFrame(
        [
            ("AB-1", "  Marie Lucie ", "1973-01-27", "m@x.fr", None,
             "ACTIVE", "2024-01-05 13:05:01", None),
            ("AB-1", "  Marie Lucie ", "1973-01-27", "m@x.fr", None,
             "ACTIVE", "2024-01-05 13:05:01", None),   # doublon volontaire
            ("AP-1", "Jean Dupont", "1980-02-02", "j@x.fr", "0600000000",
             "SUSPENDED", "2023-08-07 21:31:33", 74),
        ],
        "client_id string, nom_prenom string, date_naissance string, email string, "
        "telephone string, statut_client string, date_creation string, loyalty_score int",
    )

    lignes = nett.preparer_clients(clients).collect()
    par_id = {r["client_id"]: r for r in lignes}

    assert len(lignes) == 2                               # le doublon a disparu
    assert par_id["AB-1"]["nom_prenom"] == "Marie Lucie"  # espaces enlevés
    assert par_id["AB-1"]["statut_client"] == "ACTIF"     # ACTIVE traduit
    assert par_id["AP-1"]["statut_client"] == "SUSPENDU"  # SUSPENDED traduit


def test_preparer_contrats(spark):
    contrats = spark.createDataFrame(
        [
            # AbAssurance : prime annuelle et type déjà renseignés
            ("AB-1", "AB-1", "AUTO", None, "2024-01-01", "2024-12-31", 1200.0, None, "ACTIF"),
            ("AB-1", "AB-1", "AUTO", None, "2024-01-01", "2024-12-31", 1200.0, None, "ACTIF"),  # doublon
            # AssurePlus : seulement une prime mensuelle et un code produit
            ("AP-1", "AP-1", "", "AUTO_BASIC", "2024-04-16", "2025-04-16", None, 100.0, "ACTIVE"),
            ("AP-2", "AP-2", "", "ASSIST_PREMIUM", "2024-01-01", "2024-01-11", None, 50.0, "TERMINATED"),
        ],
        "contrat_id string, client_id string, type_assurance string, code_produit string, "
        "date_debut string, date_fin string, prime_annuelle double, prime_mensuelle double, "
        "statut_contrat string",
    )

    resultat = nett.preparer_contrats(contrats)
    lignes = {r["contrat_id"]: r for r in resultat.collect()}

    assert resultat.count() == 3   # le doublon AB-1 a disparu
    assert resultat.columns == [
        "contrat_id", "client_id", "famille_produit", "prime_annuelle_eq",
        "statut_contrat", "date_debut", "date_fin", "duree_contrat_jours",
    ]

    # AbAssurance : valeurs reprises, durée = du 1er janvier au 31 décembre 2024
    assert lignes["AB-1"]["famille_produit"] == "AUTO"
    assert lignes["AB-1"]["prime_annuelle_eq"] == 1200.0
    assert lignes["AB-1"]["duree_contrat_jours"] == 365

    # AssurePlus : famille déduite du code produit, prime mensuelle x 12
    assert lignes["AP-1"]["famille_produit"] == "AUTO"
    assert lignes["AP-1"]["prime_annuelle_eq"] == 1200.0
    assert lignes["AP-1"]["statut_contrat"] == "ACTIF"

    # ASSIST est une abréviation : on écrit le mot complet
    assert lignes["AP-2"]["famille_produit"] == "ASSISTANCE"
    assert lignes["AP-2"]["prime_annuelle_eq"] == 600.0
    assert lignes["AP-2"]["statut_contrat"] == "RESILIE"
    assert lignes["AP-2"]["duree_contrat_jours"] == 10


def test_preparer_paiements(spark):
    paiements = spark.createDataFrame(
        [
            ("p1", "c1", 100.0, "SUCCESS"),
            ("p2", "c1", 50.5, "FAILED"),
            ("p3", "c1", 50.0, "FAILED"),
            ("p3", "c1", 50.0, "FAILED"),   # doublon volontaire
            ("p4", "c2", 20.0, "PENDING"),
        ],
        "paiement_id string, contrat_id string, montant double, statut_transaction string",
    )

    resultat = {r["contrat_id"]: r for r in nett.preparer_paiements(paiements).collect()}

    # Contrat c1 : 3 paiements (le doublon n'est pas compté), 200,5 euros, 2 échoués
    assert resultat["c1"]["nb_paiements"] == 3
    assert resultat["c1"]["total_paye"] == 200.5
    assert resultat["c1"]["nb_paiements_echoues"] == 2

    # Contrat c2 : 1 paiement en attente, aucun échec
    assert resultat["c2"]["nb_paiements"] == 1
    assert resultat["c2"]["nb_paiements_echoues"] == 0


def test_preparer_sinistres(spark):
    sinistres = spark.createDataFrame(
        [
            ("s1", "c1", 1000.0, "REJECTED"),
            ("s2", "c1", 500.0, "CLOSED"),
            ("s3", "c2", 200.0, "REPORTED"),
            ("s3", "c2", 200.0, "REPORTED"),   # doublon volontaire
        ],
        "sinistre_id string, contrat_id string, montant_estime double, statut_sinistre string",
    )

    resultat = {r["contrat_id"]: r for r in nett.preparer_sinistres(sinistres).collect()}

    # Contrat c1 : 2 sinistres, 1500 euros au total, 1 rejeté
    assert resultat["c1"]["nb_sinistres"] == 2
    assert resultat["c1"]["total_sinistres"] == 1500.0
    assert resultat["c1"]["nb_sinistres_rejetes"] == 1

    # Contrat c2 : le doublon n'est pas compté
    assert resultat["c2"]["nb_sinistres"] == 1
    assert resultat["c2"]["nb_sinistres_rejetes"] == 0