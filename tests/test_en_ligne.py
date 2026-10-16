# test_en_ligne.py — tests de bout en bout de en_ligne/app.py (version en ligne, pandas, sans Spark).
#
# AppTest de Streamlit joue le rôle d'un navigateur « robot » : il lance l'application,
# remplit le formulaire de connexion, clique sur les boutons, puis on vérifie ce qui s'affiche.


import json
import sys
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from outils_auth import hacher_mot_de_passe  # noqa: E402

APP = str(Path(__file__).resolve().parent.parent / "en_ligne" / "app.py")


# ------------------------------------------------------------------
# Préparation : de fausses données et de faux services
# ------------------------------------------------------------------
@pytest.fixture
def donnees(tmp_path, monkeypatch):
    """Crée un petit dossier « donnees » dans un dossier temporaire.
    Les vrais fichiers de en_ligne/donnees ne sont jamais touchés."""
    # lire() est mise en cache (@st.cache_data) et le cache ne retient que le NOM de la table,
    # pas le dossier. Tous les tests tournent dans le même programme : sans ce ménage,
    # un test pourrait recevoir les tableaux gardés en mémoire par le test précédent.
    # C'est comme vider le frigo entre deux recettes pour ne pas cuisiner avec les restes.
    import streamlit as st
    st.cache_data.clear()

    tables = {
        "clients": pd.DataFrame({"client_id": ["AB-1"], "code_postal": ["29617"],
                                 "date_creation": ["2024-01-05"], "statut_client": ["ACTIF"]}),
        "contrats": pd.DataFrame({"contrat_id": ["C1", "C2", "C3"], "statut_contrat": ["ACTIF", "ACTIF", None],
                                  "type_assurance": ["AUTO", "HABITATION", "AUTO"]}),
        "paiements": pd.DataFrame({"paiement_id": ["1"], "montant": ["10"]}),
        "sinistres": pd.DataFrame({"sinistre_id": ["1", "2"], "contrat_id": ["C1", "C2"],
                                   "date_sinistre": ["2024-11-14", "2024-12-01"],
                                   "montant_estime": ["100.5", "200"], "statut_sinistre": ["CLOTURE", "OUVERT"]}),
        # 2 sinistres : l'un à 90 % de probabilité de fraude, l'autre à 20 %
        "predictions_fraude": pd.DataFrame({"sinistre_id": ["1", "2"], "proba_fraude": [0.9, 0.2]}),
        "metriques_fraude": pd.DataFrame({"nb_lignes": [37], "nb_suspects": [7],
                                          "precision_suspects": [0.08], "rappel_suspects": [0.14]}),
    }
    for nom, table in tables.items():
        (tmp_path / nom).mkdir()
        table.to_parquet(tmp_path / nom / "part-0.parquet")

    # L'application lit ses fichiers dans le dossier indiqué par DOSSIER_DONNEES
    monkeypatch.setenv("DOSSIER_DONNEES", str(tmp_path))

    # Les tests utilisent UNIQUEMENT leurs propres comptes (mdp-admin / mdp-user).
    # Streamlit lit aussi le fichier .streamlit/secrets.toml du dossier courant : s'il existe
    # (celui créé pour tester le déploiement), les VRAIS comptes passeraient devant ceux du test.
    # On se place donc dans le dossier temporaire, où ce fichier n'existe pas,
    # et on retire une éventuelle variable AUTH_USERS de l'ordinateur.
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AUTH_USERS", raising=False)

    # Les services pointent vers 127.0.0.1 (notre propre machine), où rien n'écoute :
    # la porte est refusée tout de suite, au lieu d'attendre la fin d'un délai.
    monkeypatch.setenv("HOTE_KAFKA", "127.0.0.1")
    monkeypatch.setenv("HOTE_NAMENODE", "127.0.0.1")

    # On fait comme si pyspark n'était pas installé, comme sur Streamlit Cloud.
    # (Mettre None dans sys.modules fait échouer « import pyspark » avec ImportError.)
    # Sans ça, sur un PC où pyspark est installé, le test démarrerait un vrai Spark (~20 s).
    monkeypatch.setitem(sys.modules, "pyspark", None)
    monkeypatch.setitem(sys.modules, "pyspark.sql", None)
    return tmp_path


def _appli():
    """Lance l'application avec deux comptes de test (même format que AUTH_USERS)."""
    at = AppTest.from_file(APP, default_timeout=30)
    at.secrets["AUTH_USERS"] = json.dumps({
        "Aurelie": {"hash": hacher_mot_de_passe("mdp-admin"), "role": "admin"},
        "Lilou": {"hash": hacher_mot_de_passe("mdp-user"), "role": "user"},
    })
    return at.run()


def _connecter(login, mdp):
    """Remplit le formulaire de connexion et clique sur « Se connecter »."""
    at = _appli()
    at.text_input[0].set_value(login)
    at.text_input[1].set_value(mdp)
    at.button[0].click()
    return at.run()


def _metriques(at):
    """Toutes les cartes chiffrées de la page, sous forme {titre: valeur}."""
    return {m.label: m.value for m in at.metric}


# ------------------------------------------------------------------
# Connexion
# ------------------------------------------------------------------
def test_formulaire_de_connexion(donnees):
    assert _appli().title[0].value == "Connexion"


def test_sans_compte_configure_l_application_reste_fermee(donnees):
    # Pas de AUTH_USERS : l'application doit refuser de s'ouvrir
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert any("Aucun compte" in e.value for e in at.error)


@pytest.mark.parametrize("login,mdp", [("Lilou", "faux"), ("inconnu", "mdp-user"), ("", "")])
def test_connexion_refusee(donnees, login, mdp):
    at = _connecter(login, mdp)
    assert at.title[0].value == "Connexion"
    assert any("incorrect" in e.value for e in at.error)


def test_deconnexion_ramene_au_formulaire(donnees):
    at = _connecter("Lilou", "mdp-user")
    at.sidebar.button[0].click()   # le bouton « Se déconnecter » est dans la barre latérale
    at = at.run()
    assert at.title[0].value == "Connexion"


# ------------------------------------------------------------------
# Onglets selon le rôle
# ------------------------------------------------------------------
def test_user_voit_le_tableau_de_bord_et_la_fraude(donnees):
    at = _connecter("Lilou", "mdp-user")
    assert not at.exception
    assert [t.label for t in at.tabs] == ["Tableau de bord", "Fraude"]


def test_admin_voit_aussi_les_services(donnees):
    at = _connecter("Aurelie", "mdp-admin")
    assert not at.exception
    assert [t.label for t in at.tabs] == ["Tableau de bord", "Fraude", "Services"]


# ------------------------------------------------------------------
# Tableau de bord
# ------------------------------------------------------------------
def test_chiffres_cles(donnees):
    valeurs = _metriques(_connecter("Lilou", "mdp-user"))
    assert valeurs["Clients"] == "1"
    assert valeurs["Contrats"] == "3"
    assert valeurs["Paiements"] == "1"
    assert valeurs["Sinistres"] == "2"


def test_contrats_par_statut(donnees):
    at = _connecter("Lilou", "mdp-user")
    assert _metriques(at)["Nombre de contrats « ACTIF »"] == "2"
    # Le statut vide est affiché « (vide) » et peut être choisi dans le menu
    menu = [s for s in at.selectbox if s.label == "Quel statut veux-tu regarder ?"][0]
    menu.select("(vide)")
    at = at.run()
    assert _metriques(at)["Nombre de contrats « (vide) »"] == "1"


def test_montant_total_des_sinistres(donnees):
    # 100,5 + 200 = 300,5 (les montants sont du texte dans les données : l'appli les convertit)
    assert _metriques(_connecter("Lilou", "mdp-user"))["Montant total estimé des sinistres"] == "300.5"


def test_un_bouton_d_export_par_table(donnees):
    at = _connecter("Lilou", "mdp-user")
    textes = " ".join(str(element.proto) for element in at.get("download_button"))
    for table in ["clients", "contrats", "paiements", "sinistres"]:
        assert "Télécharger " + table + ".csv" in textes


def test_fichiers_de_donnees_absents(donnees, monkeypatch, tmp_path):
    # Dossier vide : message d'erreur clair au lieu d'un plantage
    vide = tmp_path / "vide"
    vide.mkdir()
    monkeypatch.setenv("DOSSIER_DONNEES", str(vide))
    at = _connecter("Lilou", "mdp-user")
    assert any("Impossible de lire les fichiers" in e.value for e in at.error)


# ------------------------------------------------------------------
# Fraude
# ------------------------------------------------------------------
def test_qualite_du_modele(donnees):
    valeurs = _metriques(_connecter("Lilou", "mdp-user"))
    assert valeurs["Sinistres d'entraînement"] == "37"
    assert valeurs["dont suspects"] == "7"
    assert valeurs["Précision (suspects)"] == "8%"
    assert valeurs["Rappel (suspects)"] == "14%"


def test_seuil_de_probabilite(donnees):
    at = _connecter("Lilou", "mdp-user")
    # Seuil par défaut 0,5 : seul le sinistre à 90 % est signalé
    assert _metriques(at)["Sinistres signalés"] == "1 sur 2"
    # Seuil abaissé à 0,1 : les deux sinistres sont signalés
    at.slider[0].set_value(0.1)
    at = at.run()
    assert _metriques(at)["Sinistres signalés"] == "2 sur 2"


def test_resultats_de_fraude_absents(donnees):
    # Sans le dossier des prédictions, la page explique ce qui manque au lieu de planter
    import shutil
    shutil.rmtree(donnees / "predictions_fraude")
    at = _connecter("Lilou", "mdp-user")
    assert not at.exception
    assert any("modèle de fraude" in i.value for i in at.info)


# ------------------------------------------------------------------
# Services (admin)
# ------------------------------------------------------------------
def _cliquer_verifier(at):
    [b for b in at.button if b.label == "Vérifier les services"][0].click()
    return at.run()


def test_services_attendent_le_clic(donnees):
    # Tant qu'on n'a pas cliqué, aucune vérification n'est lancée
    at = _connecter("Aurelie", "mdp-admin")
    assert any("Vérifier les services" in i.value for i in at.info)


def test_services_injoignables_declenchent_l_alerte(donnees):
    at = _cliquer_verifier(_connecter("Aurelie", "mdp-admin"))
    assert not at.exception
    alertes = " ".join(e.value for e in at.error)
    for service in ["Kafka", "Hadoop", "Streaming", "Spark"]:
        assert service in alertes


def test_streaming_non_verifie_si_hadoop_est_absent(donnees):
    # Inutile d'ouvrir les tiroirs si le bibliothécaire (le namenode) est absent
    at = _cliquer_verifier(_connecter("Aurelie", "mdp-admin"))
    legendes = " ".join(c.value for c in at.caption)
    assert "non vérifié : Hadoop injoignable" in legendes
    assert "Spark n'est pas installé" in legendes


def test_resultat_des_services_garde_apres_un_autre_clic(donnees):
    # Le résultat est rangé en mémoire de session : il reste affiché
    # quand l'admin interagit ailleurs (ici, le curseur de la page fraude).
    at = _cliquer_verifier(_connecter("Aurelie", "mdp-admin"))
    at.slider[0].set_value(0.1)
    at = at.run()
    assert any("Dernière vérification" in c.value for c in at.caption)