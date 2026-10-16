# app.py — Tableau de bord AbAssurance EN LIGNE (version simple : pandas, sans Spark ni Hadoop).
# Les chiffres viennent de fichiers exportés depuis HDFS, rangés dans le dossier « donnees ».
# La version complète (Docker / Spark / Hadoop) est dans le dossier app/.

import json                 # pour lire les réponses du namenode
import os
import sys
import time                 # pour chronométrer chaque vérification
import urllib.request       # pour web du namenode
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st


# On réutilise la connexion du projet (dossier app/) : mêmes comptes, même AUTH_USERS qu'en Docker.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))
from auth import bouton_deconnexion, exiger_connexion
from outils import tester_porte, interpreter_datanodes, trouver_pannes, probas_en_pourcentage
from outils_auth import pages_autorisees  

st.set_page_config(page_title="Tableau de bord AbAssurance", layout="wide")

# Dossier des fichiers de données (changeable avec la variable DOSSIER_DONNEES, utile pour les tests)
DOSSIER = Path(os.environ.get("DOSSIER_DONNEES", Path(__file__).parent / "donnees"))

# Connexion obligatoire : tant que personne n'est connecté, le reste du fichier ne s'exécute pas.
exiger_connexion()


# ------------------------------------------------------------------
# 1. Lecture des données
# ------------------------------------------------------------------
@st.cache_data
def lire(nom_table):
    return pd.read_parquet(DOSSIER / nom_table)


def csv_pour_excel(tableau):
    """CSV avec « ; » et accents, qui s'ouvre correctement dans Excel en français."""
    return tableau.to_csv(index=False, sep=";").encode("utf-8-sig")


# ------------------------------------------------------------------
# 2. Les pages
# ------------------------------------------------------------------
def page_tableau_de_bord():
    st.title("Tableau de bord AbAssurance / AssurePlus")
    st.caption("Données de démonstration exportées de Hadoop.")
    try:
        clients, contrats = lire("clients"), lire("contrats")
        paiements, sinistres = lire("paiements"), lire("sinistres")
    except Exception as erreur:
        st.error("Impossible de lire les fichiers de données (dossier « donnees »).")
        st.exception(erreur)
        st.stop()

    # Chiffres clés
    st.subheader("Chiffres clés")
    for colonne, (titre, table) in zip(st.columns(4), [
        ("Clients", clients), ("Contrats", contrats), ("Paiements", paiements), ("Sinistres", sinistres),
    ]):
        colonne.metric(titre, len(table))

    # Contrats : on choisit un statut, on voit le nombre et la répartition par type d'assurance
    st.subheader("Contrats")
    par_statut = contrats["statut_contrat"].fillna("(vide)").value_counts()
    choix = st.selectbox("Quel statut veux-tu regarder ?", list(par_statut.index))
    st.metric("Nombre de contrats « " + str(choix) + " »", int(par_statut[choix]))
    contrats_du_statut = contrats[contrats["statut_contrat"].fillna("(vide)") == choix]
    st.bar_chart(contrats_du_statut["type_assurance"].fillna("(non renseigné)").value_counts())

    # Sinistres
    st.subheader("Sinistres")
    total = pd.to_numeric(sinistres["montant_estime"], errors="coerce").sum()
    st.metric("Montant total estimé des sinistres", round(float(total), 2))

    # Exports CSV (sans les colonnes sensibles : pas d'email, téléphone, adresse...)
    st.subheader("Exporter les données")
    tables = {
        "clients": clients[["client_id", "code_postal", "date_creation", "statut_client"]],
        "contrats": contrats,
        "paiements": paiements,
        "sinistres": sinistres[["sinistre_id", "contrat_id", "date_sinistre", "montant_estime", "statut_sinistre"]],
    }
    for colonne, (nom, table) in zip(st.columns(4), tables.items()):
        colonne.download_button("Télécharger " + nom + ".csv", csv_pour_excel(table), nom + ".csv", "text/csv")


def page_fraude():
    st.title("Détection de fraude")
    if not (DOSSIER / "predictions_fraude").exists():
        st.info("Les résultats du modèle de fraude ne sont pas dans le dossier « donnees ».")
        return
    predictions, metriques = lire("predictions_fraude"), lire("metriques_fraude").iloc[0]

    st.subheader("Qualité du modèle")
    cartes = [
        ("Sinistres d'entraînement", int(metriques["nb_lignes"])),
        ("dont suspects", int(metriques["nb_suspects"])),
        ("Précision (suspects)", f"{metriques['precision_suspects']:.0%}"),
        ("Rappel (suspects)", f"{metriques['rappel_suspects']:.0%}"),
    ]
    for colonne, (titre, valeur) in zip(st.columns(4), cartes):
        colonne.metric(titre, valeur)
    st.warning("Le jeu d'entraînement est très petit : ces chiffres sont indicatifs.")

    st.subheader("Sinistres à surveiller")
    seuil = st.slider("Probabilité de fraude d'au moins", 0.0, 1.0, 0.5, 0.05)
    a_surveiller = predictions[predictions["proba_fraude"] >= seuil].sort_values("proba_fraude", ascending=False)
    st.metric("Sinistres signalés", str(len(a_surveiller)) + " sur " + str(len(predictions)))
    # À l'écran : la probabilité (0.875) s'affiche directement en pourcentage (87.5 %),
    # sans changer les données elles-mêmes (seulement la façon de les montrer).
    st.dataframe(
        a_surveiller,
        hide_index=True,
        column_config={
            "proba_fraude": st.column_config.NumberColumn("Probabilité de fraude", format="percent")
        },
    )
    # Dans le fichier CSV : la probabilité devient un pourcentage entier (88)
    st.download_button(
        "Télécharger la liste (CSV)",
        csv_pour_excel(probas_en_pourcentage(a_surveiller)),
        "sinistres_a_surveiller.csv",
        "text/csv",
    )

# ------------------------------------------------------------------
# Page « Services » (admin) : version allégée de Etat_des_services.py
# ------------------------------------------------------------------
# Pourquoi ne pas simplement importer Etat_des_services.py ?
# Parce que ce fichier exécute tout dès qu'on l'ouvre (titre, vérifications, mails...).
# On recopie donc ici seulement les vérifications, rangées dans une fonction.

# Les adresses des services. On peut les changer avec des variables d'environnement
# (utile si on lance l'appli hors du réseau Docker).
HOTE_KAFKA = os.environ.get("HOTE_KAFKA", "kafka")
HOTE_NAMENODE = os.environ.get("HOTE_NAMENODE", "namenode")


def _verifier_kafka():
    # 19092 = la port INTERNE de Kafka dans le réseau Docker.
    if tester_porte(HOTE_KAFKA, 19092):
        return True, "Kafka répond"
    return False, "Kafka ne répond pas"


def _verifier_hadoop():
    # On demande au namenode de Hadoop combien de datanodes
    adresse = ("http://" + HOTE_NAMENODE + ":9870/jmx"
               "?qry=Hadoop:service=NameNode,name=FSNamesystemState")
    try:
        reponse = urllib.request.urlopen(adresse, timeout=3)
        infos = json.loads(reponse.read())["beans"][0]
        vivants = infos["NumLiveDataNodes"]
        silencieux = infos.get("NumStaleDataNodes", 0)   # pas de nouvelles depuis 30 s
    except Exception as erreur:
        # Pas de reponse
        return False, "namenode injoignable : " + str(erreur)
    # Même règle que la version Docker : si au moins un datanode est vivant et aucun n'est silencieux, c'est OK.
    return interpreter_datanodes(vivants, silencieux)


def _verifier_streaming():
    # On regarde si chaque table Kafka (clients, contrats, paiements, sinistres) contient au moins un fichier Parquet.
    vides = []
    for table in ["clients", "contrats", "paiements", "sinistres"]:
        adresse = ("http://" + HOTE_NAMENODE + ":9870/webhdfs/v1/data/kafka/"
                   + table + "?op=LISTSTATUS")
        try:
            reponse = urllib.request.urlopen(adresse, timeout=3)
            fichiers = json.loads(reponse.read())["FileStatuses"]["FileStatus"]
            if len(fichiers) == 0:
                vides.append(table)
        except Exception:
            vides.append(table)
    if len(vides) == 0:
        return True, "les 4 tables contiennent des fichiers"
    return False, "table vide ou introuvable : " + ", ".join(vides)


def _verifier_spark():
    # La version en ligne n'installe pas forcément pyspark.
    # On l'importe ICI, dans la fonction : si pyspark est absent,
    # seule cette vérification échoue, pas toute l'appli.
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return False, "Spark n'est pas installé sur cette version"
    try:
        # Petit calcul test : compter jusqu'à 10. S'il répond juste, il tourne.
        spark = SparkSession.builder.master("local[2]").appName("EtatServices").getOrCreate()
        if spark.range(10).count() == 10:
            return True, "Spark répond"
        return False, "Spark donne un mauvais résultat"
    except Exception as erreur:
        return False, "Spark en panne : " + str(erreur)


def page_services():
    st.title("État des services du pipeline")
    st.caption("Kafka → Hadoop → Spark")

    # IMPORTANT : avec st.tabs, Streamlit exécute TOUS les onglets à chaque clic,
    # même ceux qu'on ne regarde pas. Sans bouton, chaque clic sur le tableau de bord
    # relancerait les vérifications (plusieurs secondes d'attente).
    # On attend donc que l'admin appuie sur le bouton.
    if st.button("Vérifier les services"):
        etapes = [
            ("Kafka", _verifier_kafka),
            ("Hadoop", _verifier_hadoop),
            ("Streaming", _verifier_streaming),
            ("Spark", _verifier_spark),
        ]
        resultats = []   # on y range (nom, ok, message, durée)
        with st.spinner("Vérification en cours..."):
                for nom, fonction in etapes:
                # Si le namenode (le bibliothécaire) est absent, inutile
                # d'aller ouvrir les tiroirs un par un : on sait déjà que c'est fermé.
                    if nom == "Streaming" and not resultats[1][1]:
                        resultats.append((nom, False, "non vérifié : Hadoop injoignable", 0.0))
                        continue
                    debut = time.time()
                    ok, message = fonction()
                    duree = time.time() - debut
                    resultats.append((nom, ok, message, duree))
        # On garde le résultat en mémoire de session :
        # il reste affiché quand on change d'onglet et qu'on revient.
        st.session_state["etat_services"] = {
            "date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "resultats": resultats,
        }

    etat = st.session_state.get("etat_services")
    if etat is None:
        st.info("Clique sur « Vérifier les services » pour lancer les contrôles.")
        return

    st.caption("Dernière vérification : " + etat["date"])
    resultats = etat["resultats"]

    # La bannière d'alerte (trouver_pannes attend des triplets (nom, ok, message))
    noms_en_panne = trouver_pannes([(nom, ok, msg) for nom, ok, msg, _ in resultats])
    if noms_en_panne:
        st.error("🚨 ALERTE : problème sur " + ", ".join(noms_en_panne))
    else:
        st.success("✅ Tout fonctionne")

    # Les voyants : 5 cartes côte à côte, comme sur un tableau de bord de voiture
    colonnes = st.columns(5)
    colonnes[0].markdown("### ⚪ Talend")
    colonnes[0].caption("Non surveillable ici (il tourne sur le PC)")
    for colonne, (nom, ok, message, duree) in zip(colonnes[1:], resultats):
        voyant = "🟢" if ok else "🔴"
        colonne.markdown("### " + voyant + " " + nom)
        colonne.caption(message + " (" + str(round(duree, 1)) + " s)")


# ------------------------------------------------------------------
# 3. Programme principal : un onglet par page autorisée pour le rôle
# ------------------------------------------------------------------
bouton_deconnexion()

# Le « plan du bâtiment » : pour chaque page, le titre de son onglet et la fonction qui l'affiche
PAGES = {
    "dashboard": ("Tableau de bord", page_tableau_de_bord),
    "fraude": ("Fraude", page_fraude),
    "services": ("Services", page_services),
}

# Les pages que ce rôle a le droit de voir (par exemple user -> ["dashboard", "fraude"])
pages_du_role = pages_autorisees(st.session_state["role"])

# On crée un onglet par page autorisée...
onglets = st.tabs([PAGES[cle][0] for cle in pages_du_role])

# ...puis on remplit chaque onglet avec sa page (zip = on avance dans les 2 listes en même temps)
for onglet, cle in zip(onglets, pages_du_role):
    titre, afficher_page = PAGES[cle]
    with onglet:
        afficher_page()