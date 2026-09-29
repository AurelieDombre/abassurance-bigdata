# app.py — Version allégée du tableau de bord (US 5.1)


import time

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# L'adresse du "rayon" de l'entrepôt où le streaming (US 4.2) a rangé les données.
DOSSIER_HDFS = "hdfs://namenode:9000/data/kafka"

st.set_page_config(page_title="Tableau de bord AbAssurance", layout="wide")


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
    """Transforme un petit résultat Spark en tableau pandas, pour l'afficher."""
    lignes = []
    for ligne in resultat_spark.collect():   # collect() = "toutes les lignes"
        lignes.append(ligne.asDict())        # asDict() = la ligne sous forme de dictionnaire
    return pd.DataFrame(lignes, columns=resultat_spark.columns)


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

    # --- Échantillons de 10 lignes à comparer avec les fichiers sources.
    # Pour les clients et les sinistres, on ne garde que les colonnes NON sensibles (pas d'email, de téléphone, d'adresse, de numéro fiscal, ni de description).
    echantillon_clients = en_tableau(
        clients.select("client_id", "code_postal", "date_creation", "statut_client").limit(10)
    )
    echantillon_contrats = en_tableau(contrats.limit(10))
    echantillon_paiements = en_tableau(paiements.limit(10))
    echantillon_sinistres = en_tableau(
        sinistres.select(
            "sinistre_id", "contrat_id", "date_sinistre", "montant_estime", "statut_sinistre"
        ).limit(10)
    )

    duree = time.time() - debut   # on arrête le chrono

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
        "echantillons": {
            "clients": echantillon_clients,
            "contrats": echantillon_contrats,
            "paiements": echantillon_paiements,
            "sinistres": echantillon_sinistres,
        },
    }
    return resultats


# ------------------------------------------------------------------
# L'affichage
# ------------------------------------------------------------------
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

# Le temps de traitement (critère 2 de l'US 5.1)
st.info("Temps de traitement Spark : " + str(round(resultats["duree"], 1)) + " secondes")

# --- Les contrats, avec le menu déroulant des statuts
st.subheader("Contrats")
statuts = list(resultats["contrats_par_statut"].keys())   # la liste des statuts trouvés

if len(statuts) > 0:
    choix = st.selectbox("Quel statut veux-tu regarder ?", statuts)

    # On affiche d'abord le nombre total de contrats pour ce statut
    st.metric("Nombre de contrats « " + choix + " »", resultats["contrats_par_statut"][choix])

    # Puis on ouvre le "tiroir" de ce statut : le détail par type d'assurance.
    # C'est CE dictionnaire qui change selon le choix fait dans le menu déroulant,
    # donc le graphique en dessous change lui aussi.
    detail_du_statut = resultats["par_statut_et_type"].get(choix, {})

    if len(detail_du_statut) > 0:
        st.caption("Répartition des contrats « " + choix + " » par type d'assurance :")
        # pd.Series transforme le dictionnaire {"auto": 300, "habitation": 200}
        # en une petite colonne que st.bar_chart sait dessiner.
        st.bar_chart(pd.Series(detail_du_statut))
    else:
        st.warning("Aucun type d'assurance trouvé pour ce statut.")
else:
    st.warning("Aucun contrat trouvé dans Hadoop.")

# --- Les sinistres
st.subheader("Sinistres")
st.metric("Montant total estimé des sinistres", round(resultats["total_sinistres"], 2))

# --- Les échantillons à vérifier (critère 3 de l'US 5.1)
st.subheader("Échantillons à vérifier à la main")
st.caption("Compare ces lignes avec tes fichiers d'extraction pour confirmer qu'elles sont correctes.")

# .items() donne à chaque tour de boucle le nom (clients, contrats...) et son tableau
for nom, tableau in resultats["echantillons"].items():
    with st.expander("10 premières lignes : " + nom):
        st.dataframe(tableau)