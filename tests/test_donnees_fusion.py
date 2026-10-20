"""Contrôle de cohérence des CLIENTS fusionnés (AbAssurance + AssurePlus).

Pourquoi ce test ? Un bug a inversé les colonnes statut_client et
date_creation pour les clients AssurePlus dans le job Talaxie de fusion
(transformation_client). Exemple : AP-1 avait date_creation = "ACTIVE"
et statut_client = "2023-08-07 21:31:33".
"""

import csv
import re
from pathlib import Path

import pytest

PROJET = Path(__file__).resolve().parent.parent

# Fichier produit par le job Talaxie transformation_client
FICHIER_FUSION = PROJET / "data" / "output" / "dataClean_fusion" / "clients"
# Données exportées pour la version en ligne (Streamlit Community Cloud)
DOSSIER_EN_LIGNE = PROJET / "en_ligne" / "donnees" / "clients"

# Statuts acceptés : français (AbAssurance) et anglais (AssurePlus, avant
# la normalisation faite plus loin dans le pipeline)
STATUTS_VALIDES = {"ACTIF", "INACTIF", "SUSPENDU", "ACTIVE", "INACTIVE", "SUSPENDED"}

# Une date de création ressemble à « 2023-08-07 » ou « 2023-08-07 21:31:33 »
FORMAT_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$")


def _lignes_incoherentes(lignes):
    """Renvoie la liste des clients dont le statut ou la date est mal rangé."""
    erreurs = []
    for ligne in lignes:
        statut = (ligne["statut_client"] or "").strip().upper()
        date = (ligne["date_creation"] or "").strip()
        if statut not in STATUTS_VALIDES or not FORMAT_DATE.match(date):
            erreurs.append(f'{ligne["client_id"]} : statut="{statut}" date="{date}"')
    return erreurs


def test_lignes_incoherentes_detecte_une_inversion():
    # On vérifie d'abord que le contrôle lui-même fonctionne :
    # une ligne correcte passe, une ligne inversée est repérée.
    bonne = {"client_id": "AB-1", "statut_client": "ACTIF", "date_creation": "2024-01-05 13:05:01"}
    inversee = {"client_id": "AP-1", "statut_client": "2023-08-07 21:31:33", "date_creation": "ACTIVE"}

    assert _lignes_incoherentes([bonne]) == []
    assert len(_lignes_incoherentes([bonne, inversee])) == 1


def test_fichier_fusion_clients_statut_et_date_bien_ranges():
    # On lit le fichier en se fiant à l'EN-TÊTE (noms de colonnes),
    # pas à la position : c'est justement la position qui posait problème.
    with open(FICHIER_FUSION, encoding="utf-8", newline="") as fichier:
        lignes = list(csv.DictReader(fichier, delimiter=";"))

    erreurs = _lignes_incoherentes(lignes)

    assert len(lignes) > 0
    # On affiche au plus 5 exemples pour garder un message lisible
    assert erreurs == [], f"{len(erreurs)} clients mal rangés, ex. : {erreurs[:5]}"


def test_donnees_en_ligne_clients_statut_et_date_bien_ranges():
    # Même contrôle sur les données de la version en ligne (fichiers Parquet)
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    clients = pd.read_parquet(DOSSIER_EN_LIGNE).astype(str)
    erreurs = _lignes_incoherentes(clients.to_dict("records"))

    assert len(clients) > 0
    assert erreurs == [], f"{len(erreurs)} clients mal rangés, ex. : {erreurs[:5]}"
