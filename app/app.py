# app.py — Tableau de bord AbAssurance / AssurePlus (US 5.1)

import time
from datetime import datetime

import pandas as pd
import streamlit as st
from fpdf import FPDF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from outils_perf import noter_mesure

# Config de la page : une seule fois, tout en haut
st.set_page_config(page_title="Tableau de bord AbAssurance", layout="wide")

DOSSIER_HDFS = "hdfs://namenode:9000/data/kafka"


# ------------------------------------------------------------------
# Préparation de Spark
# ------------------------------------------------------------------

#  @st.cache_resource permet à Streamlit de garder l'objet en mémoire, ne pas le recrée pas à chaque clic.
@st.cache_resource
def demarrer_spark():
    spark = (
        SparkSession.builder.appName("DashboardAbAssurance")
        .master("local[2]")                    # on utilise 2 cœurs seulement
        .config("spark.driver.memory", "1g")   # et 1 Go de mémoire maximum
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    return spark


def lire(spark, nom_table):
    """Lire une table (clients, contrats...) dans Hadoop."""
    return spark.read.parquet(DOSSIER_HDFS + "/" + nom_table)


def en_tableau(resultat_spark):
    """Transforme un résultat Spark en tableau pandas, pour l'afficher ou l'exporter."""
    lignes = []
    for ligne in resultat_spark.collect():   # collect() = "toutes les lignes"
        lignes.append(ligne.asDict())        # asDict() = la ligne sous forme de dictionnaire
    return pd.DataFrame(lignes, columns=resultat_spark.columns)


def vers_csv(tableau_pandas):
    """Transforme un tableau pandas en texte CSV, prêt à être téléchargé.
    encoding="utf-8-sig" = ajoute un petit marqueur invisible au début du
    fichier pour qu'Excel affiche bien les accents (é, è, à...) sans bug."""
    return tableau_pandas.to_csv(index=False, sep=";").encode("utf-8-sig")


def texte_pdf(texte):
    """fpdf2 (police de base) ne connaît pas tous les caractères Unicode.
    On remplace ceux qu'il ne sait pas afficher, pour ne jamais planter."""
    return texte.encode("latin-1", "replace").decode("latin-1")


# ------------------------------------------------------------------
# Les calculs (faits par Spark)
# ------------------------------------------------------------------

# @st.cache_data = tant qu'on ne clique pas sur "Recalculer", on ne refait pas tout le travail à chaque clic sur la page.
@st.cache_data(show_spinner="Spark calcule les chiffres à partir de Hadoop...")
def calculer():
    spark = demarrer_spark()
    debut = time.time()   # top chrono : on note l'heure de départ

    # On va chercher les 4 tables dans l'entrepôt
    clients = lire(spark, "clients")
    contrats = lire(spark, "contrats")
    paiements = lire(spark, "paiements")
    sinistres = lire(spark, "sinistres")

    # --- Nombre de contrats pour chaque statut
    # On construit un dictionnaire : un carnet où chaque statut a son nombre.
    # Exemple : {"ACTIF": 500, "suspendu": 30, "résilié": 120}
    contrats_par_statut = {}
    for ligne in contrats.groupBy("statut_contrat").count().collect():
        statut = ligne["statut_contrat"]
        if statut is None:            # si le statut est vide, on met une étiquette lisible
            statut = "(vide)"
        contrats_par_statut[statut] = ligne["count"]

    # --- Nombre de contrats par statut ET par type d'assurance.
    # Résultat voulu, par exemple :
    #   {
    #     "ACTIF":    {"auto": 300, "habitation": 200},
    #     "suspendu": {"auto": 20,  "habitation": 10},
    #   }
    par_statut_et_type = {}
    lignes_croisees = contrats.groupBy("statut_contrat", "type_assurance").count().collect()
    for ligne in lignes_croisees:
        statut = ligne["statut_contrat"]
        if statut is None:
            statut = "(vide)"
        type_assurance = ligne["type_assurance"]
        if type_assurance is None:
            type_assurance = "(non renseigné)"

        # Si on n'a encore jamais vu ce statut, on ouvre un tiroir vide pour lui
        if statut not in par_statut_et_type:
            par_statut_et_type[statut] = {}

        # On range le compte dans le bon tiroir
        par_statut_et_type[statut][type_assurance] = ligne["count"]

    # --- Montant total estimé des sinistres
    total_sinistres = sinistres.agg(F.sum("montant_estime")).first()[0]
    if total_sinistres is None:       # s'il n'y a aucune donnée, on met 0
        total_sinistres = 0

    # --- Tables complètes en pandas, pour l'affichage ET l'export CSV.
    # Pour clients et sinistres, on ne garde QUE les colonnes non sensibles
    # (pas d'email, de téléphone, d'adresse, de numéro fiscal, ni de texte
    # libre de description) : cohérent avec la protection des données
    # prévue à l'US 4.3.
    table_clients = en_tableau(
        clients.select("client_id", "code_postal", "date_creation", "statut_client")
    )
    table_contrats = en_tableau(contrats)
    table_paiements = en_tableau(paiements)
    table_sinistres = en_tableau(
        sinistres.select(
            "sinistre_id", "contrat_id", "date_sinistre", "montant_estime", "statut_sinistre"
        )
    )

    # --- Échantillons de 10 lignes à comparer avec les fichiers sources.
    # On les prend directement dans les tables complètes ci-dessus (.head(10)
    # avec pandas), pas besoin de redemander les données à Spark une 2e fois.
    echantillons = {
        "clients": table_clients.head(10),
        "contrats": table_contrats.head(10),
        "paiements": table_paiements.head(10),
        "sinistres": table_sinistres.head(10),
    }

    duree = time.time() - debut   # on arrête le chrono
    noter_mesure("Calcul Spark (dashboard)", duree)

    # On range tous les résultats dans un dictionnaire
    resultats = {
        "nb_clients": clients.count(),
        "nb_contrats": contrats.count(),
        "nb_paiements": paiements.count(),
        "nb_sinistres": sinistres.count(),
        "contrats_par_statut": contrats_par_statut,
        "par_statut_et_type": par_statut_et_type,
        "total_sinistres": total_sinistres,
        "duree": duree,
        # "tables" contient les tables COMPLETES : sert pour l'export CSV.
        "tables": {
            "clients": table_clients,
            "contrats": table_contrats,
            "paiements": table_paiements,
            "sinistres": table_sinistres,
        },
        # "echantillons" contient juste 10 lignes : sert pour l'aperçu à l'écran.
        "echantillons": echantillons,
    }
    return resultats


# ------------------------------------------------------------------
# Construction du rapport PDF
# ------------------------------------------------------------------
def generer_rapport_pdf(resultats):
    """Construit un petit rapport PDF avec les chiffres clés.
    On procède comme pour écrire une page à la main : on pose du texte
    ligne par ligne, du haut vers le bas."""
    pdf = FPDF()
    pdf.add_page()

    # --- Titre
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, texte_pdf("Rapport AbAssurance / AssurePlus"), ln=True)

    # --- Date de génération
    pdf.set_font("Helvetica", "", 10)
    date_du_jour = datetime.now().strftime("%d/%m/%Y a %H:%M")
    pdf.cell(0, 8, texte_pdf("Genere le " + date_du_jour), ln=True)
    pdf.cell(0, 8, texte_pdf("Temps de traitement Spark : " + str(round(resultats["duree"], 1)) + " secondes"), ln=True)
    pdf.ln(6)   # ln(6) = saut d'une petite ligne vide, pour aérer

    # --- Chiffres clés
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, texte_pdf("Chiffres cles"), ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, texte_pdf("Clients : " + str(resultats["nb_clients"])), ln=True)
    pdf.cell(0, 8, texte_pdf("Contrats : " + str(resultats["nb_contrats"])), ln=True)
    pdf.cell(0, 8, texte_pdf("Paiements : " + str(resultats["nb_paiements"])), ln=True)
    pdf.cell(0, 8, texte_pdf("Sinistres : " + str(resultats["nb_sinistres"])), ln=True)
    pdf.cell(0, 8, texte_pdf("Montant total estime des sinistres : " + str(round(resultats["total_sinistres"], 2))), ln=True)
    pdf.ln(6)

    # --- Répartition des contrats par statut
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, texte_pdf("Contrats par statut"), ln=True)
    pdf.set_font("Helvetica", "", 11)
    for statut, nombre in resultats["contrats_par_statut"].items():
        pdf.cell(0, 8, texte_pdf(statut + " : " + str(nombre)), ln=True)

    # pdf.output() renvoie le PDF sous forme de bytes, directement
    # utilisables par le bouton de téléchargement (pas besoin de fichier).
    return bytes(pdf.output())


# ------------------------------------------------------------------
# L'affichage
# ------------------------------------------------------------------
def tableau_de_bord():
    # Cette fonction est la "salle" du tableau de bord : elle ne s'exécute
    # que si l'utilisateur clique sur cette page dans le menu.
    st.title("Tableau de bord AbAssurance / AssurePlus")
    st.caption("Données lues dans Hadoop et calculées avec Spark.")

    # Un bouton dans le menu de gauche pour vider la mémoire et tout recalculer
    if st.sidebar.button("Recalculer depuis Hadoop"):
        st.cache_data.clear()

    try:
        resultats = calculer()
    except Exception as erreur:
        st.error(
            "Impossible de lire les données dans Hadoop. Vérifie que le namenode et les "
            "datanodes sont 'healthy' et que le streaming a bien écrit des fichiers "
            "Parquet dans /data/kafka/."
        )
        st.exception(erreur)
        st.stop()

    # --- Les chiffres clés, dans 4 colonnes côte à côte
    st.subheader("Chiffres clés")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Clients", resultats["nb_clients"])
    col2.metric("Contrats", resultats["nb_contrats"])
    col3.metric("Paiements", resultats["nb_paiements"])
    col4.metric("Sinistres", resultats["nb_sinistres"])

    st.info("Temps de traitement Spark : " + str(round(resultats["duree"], 1)) + " secondes")

    # --- Les contrats, avec le menu déroulant des statuts
    st.subheader("Contrats")
    statuts = list(resultats["contrats_par_statut"].keys())

    if len(statuts) > 0:
        choix = st.selectbox("Quel statut veux-tu regarder ?", statuts)
        st.metric("Nombre de contrats « " + choix + " »", resultats["contrats_par_statut"][choix])

        detail_du_statut = resultats["par_statut_et_type"].get(choix, {})
        if len(detail_du_statut) > 0:
            st.caption("Répartition des contrats « " + choix + " » par type d'assurance :")
            st.bar_chart(pd.Series(detail_du_statut))
        else:
            st.warning("Aucun type d'assurance trouvé pour ce statut.")
    else:
        st.warning("Aucun contrat trouvé dans Hadoop.")

    # --- Les sinistres
    st.subheader("Sinistres")
    st.metric("Montant total estimé des sinistres", round(resultats["total_sinistres"], 2))

    # --- Export des données (CSV)
    st.subheader("Exporter les données")
    st.caption("Un fichier CSV par table, prêt à ouvrir dans Excel.")

    col_export_1, col_export_2, col_export_3, col_export_4 = st.columns(4)
    colonnes_export = [
        (col_export_1, "clients"),
        (col_export_2, "contrats"),
        (col_export_3, "paiements"),
        (col_export_4, "sinistres"),
    ]
    for colonne, nom_table in colonnes_export:
        tableau = resultats["tables"][nom_table]
        colonne.download_button(
            label="Télécharger " + nom_table + ".csv",
            data=vers_csv(tableau),
            file_name=nom_table + ".csv",
            mime="text/csv",
        )

    # --- Export du rapport (PDF)
    st.subheader("Exporter le rapport")
    pdf_bytes = generer_rapport_pdf(resultats)
    st.download_button(
        label="Télécharger le rapport PDF",
        data=pdf_bytes,
        file_name="rapport_abassurance.pdf",
        mime="application/pdf",
    )

    # --- Les échantillons à vérifier
    st.subheader("Échantillons à vérifier à la main")
    st.caption("Compare ces lignes avec tes fichiers d'extraction pour confirmer qu'elles sont correctes.")
    for nom, tableau in resultats["echantillons"].items():
        with st.expander("10 premières lignes : " + nom):
            st.dataframe(tableau)


# ------------------------------------------------------------------
# Le panneau d'affichage (le menu de la sidebar)
# ------------------------------------------------------------------
pg = st.navigation([
    st.Page(tableau_de_bord, title="Tableau de bord", icon="📊", default=True),
    st.Page("pages/Detection_de_fraude.py", title="Détection de fraude", icon="🕵️"),
    st.Page("pages/Etat_des_services.py", title="État des services", icon="🚦"),
])

# L'aiguilleur de train : il envoie vers la page choisie
pg.run()