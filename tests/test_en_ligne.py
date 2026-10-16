# test_en_ligne.py — test de bout en bout de en_ligne/app.py (version en ligne, pandas).
# Simule un navigateur : connexion, rôles, pages.
import json
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from outils_auth import hacher_mot_de_passe  # noqa: E402

APP = str(Path(__file__).resolve().parent.parent / "en_ligne" / "app.py")


@pytest.fixture
def donnees(tmp_path, monkeypatch):
    tables = {
        "clients": pd.DataFrame({"client_id": ["AB-1"], "code_postal": ["29617"],
                                 "date_creation": ["2024-01-05"], "statut_client": ["ACTIF"]}),
        "contrats": pd.DataFrame({"contrat_id": ["C1", "C2", "C3"], "statut_contrat": ["ACTIF", "ACTIF", None],
                                  "type_assurance": ["AUTO", "HABITATION", "AUTO"]}),
        "paiements": pd.DataFrame({"paiement_id": ["1"], "montant": ["10"]}),
        "sinistres": pd.DataFrame({"sinistre_id": ["1"], "contrat_id": ["C1"], "date_sinistre": ["2024-11-14"],
                                   "montant_estime": ["100.5"], "statut_sinistre": ["CLOTURE"]}),
        "predictions_fraude": pd.DataFrame({
            "sinistre_id": ["1", "2"], "montant_estime": [5000.0, 200.0], "prime_annuelle": [500.0, 500.0],
            "jours_avant_sinistre": [30.0, 30.0], "nb_paiements": [10.0, 10.0], "nb_paiements_echoues": [0.0, 0.0],
            "proba_fraude": [0.9, 0.2]}),
        "metriques_fraude": pd.DataFrame({"nb_lignes": [37], "nb_suspects": [7],
                                          "precision_suspects": [0.8], "rappel_suspects": [0.7]}),
    }
    for nom, table in tables.items():
        (tmp_path / nom).mkdir()
        table.to_parquet(tmp_path / nom / "part-0.parquet")
    # Arbre minimal : si montant_estime (variable 0) <= 1000 -> feuille [8, 2] (20 % de fraude), sinon [1, 9] (90 %)
    (tmp_path / "modele_fraude.json").write_text(json.dumps({
        "colonnes": ["montant_estime", "prime_annuelle", "jours_avant_sinistre", "nb_paiements", "nb_paiements_echoues"],
        "importances": [1.0, 0.0, 0.0, 0.0, 0.0],
        "regles": "If (feature 0 <= 1000.0) ...",
        "noeuds": [
            {"id": 0, "stats": [9.0, 11.0], "gauche": 1, "droite": 2, "variable": 0, "seuil": 1000.0},
            {"id": 1, "stats": [8.0, 2.0], "gauche": -1, "droite": -1, "variable": -1, "seuil": None},
            {"id": 2, "stats": [1.0, 9.0], "gauche": -1, "droite": -1, "variable": -1, "seuil": None},
        ],
    }), encoding="utf-8")
    # Relevé de l'état des services : au dernier relevé, Hadoop est en alerte
    (tmp_path / "etat_services.txt").write_text(
        "2026-10-06 13:00:00 | OK | Kafka | Kafka répond\n2026-10-06 13:00:00 | OK | Hadoop | 2 datanodes sur 2\n"
        "2026-10-06 14:00:00 | OK | Kafka | Kafka répond\n2026-10-06 14:00:00 | ALERTE | Hadoop | 1 datanode sur 2\n",
        encoding="utf-8")
    # Mesures de performance : la dernière mesure de Spark est très lente
    (tmp_path / "performances.csv").write_text(
        "date,etape,duree_s\n" + "".join("2026-10-06 1" + str(i) + ":00:00,Spark," + d + "\n"
                                         for i, d in enumerate(["0.1", "0.1", "0.1", "5.0"])), encoding="utf-8")
    monkeypatch.setenv("DOSSIER_DONNEES", str(tmp_path))


def _appli():
    at = AppTest.from_file(APP, default_timeout=30)
    # Même format que Docker/auth.env : un texte JSON dans AUTH_USERS
    at.secrets["AUTH_USERS"] = json.dumps({
        "Aurelie": {"hash": hacher_mot_de_passe("mdp-admin"), "role": "admin"},
        "Lilou": {"hash": hacher_mot_de_passe("mdp-user"), "role": "user"},
    })
    return at.run()


def _connecter(login, mdp):
    at = _appli()
    at.text_input[0].set_value(login)
    at.text_input[1].set_value(mdp)
    at.button[0].click()
    return at.run()


def test_formulaire_de_connexion(donnees):
    assert _appli().title[0].value == "Connexion"


def test_sans_compte_configure_l_application_reste_fermee(donnees):
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert any("Aucun compte" in e.value for e in at.error)


@pytest.mark.parametrize("login,mdp", [("Lilou", "faux"), ("inconnu", "mdp-user"), ("", "")])
def test_connexion_refusee(donnees, login, mdp):
    at = _connecter(login, mdp)
    assert at.title[0].value == "Connexion"
    assert any("incorrect" in e.value for e in at.error)


def test_user_voit_le_tableau_de_bord_et_la_fraude(donnees):
    at = _connecter("Lilou", "mdp-user")
    assert not at.exception
    assert [t.label for t in at.tabs] == ["Tableau de bord", "Fraude"]
    valeurs = {m.label: m.value for m in at.metric}
    assert valeurs["Contrats"] == "3"
    assert valeurs["Nombre de contrats « ACTIF »"] == "2"


def test_admin_voit_aussi_l_etat_des_services(donnees):
    at = _connecter("Aurelie", "mdp-admin")
    assert not at.exception
    assert [t.label for t in at.tabs] == ["Tableau de bord", "Fraude", "État des services"]


def _evaluer(at, montant):
    at.number_input[0].set_value(montant)   # 1er champ du simulateur = montant estimé
    [b for b in at.button if b.label == "Évaluer ce sinistre"][0].click()
    return at.run()


@pytest.mark.parametrize("login,mdp", [("Lilou", "mdp-user"), ("Aurelie", "mdp-admin")])
def test_simulateur_de_fraude_pour_admin_et_user(donnees, login, mdp):
    at = _connecter(login, mdp)
    assert any(m.label == "Sinistres signalés" for m in at.metric)
    at = _evaluer(at, 5000.0)
    assert {m.label: m.value for m in at.metric}["Probabilité de fraude"] == "90%"
    at = _evaluer(at, 500.0)
    assert {m.label: m.value for m in at.metric}["Probabilité de fraude"] == "20%"


def test_etat_des_services_alerte_et_ralentissement(donnees):
    at = _connecter("Aurelie", "mdp-admin")
    assert any("Hadoop" in e.value for e in at.error)            # dernier relevé : Hadoop en alerte
    assert any("Ralentissement" in w.value for w in at.warning)  # Spark : 5 s contre 0,1 s d'habitude


def test_deconnexion_ramene_au_formulaire(donnees):
    at = _connecter("Lilou", "mdp-user")
    at.sidebar.button[0].click()
    at = at.run()
    assert at.title[0].value == "Connexion"


def test_simulateur_prerempli_avec_un_sinistre_existant(donnees):
    at = _connecter("Lilou", "mdp-user")
    choix = [s for s in at.selectbox if s.label == "Pré-remplir avec un sinistre existant"][0]
    choix.select("1")                        # le sinistre 1 a un montant de 5000 €
    at = at.run()
    assert at.number_input[0].value == 5000.0
    [b for b in at.button if b.label == "Évaluer ce sinistre"][0].click()
    at = at.run()
    assert {m.label: m.value for m in at.metric}["Probabilité de fraude"] == "90%"
