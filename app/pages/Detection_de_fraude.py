# detection_fraude.py — Page « Détection de fraude » du tableau de bord (US 5.3)

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

# --- Chemins vers les données et le modèle dans Hadoop
HDFS = "hdfs://namenode:9000"
DOSSIER = HDFS + "/data/clean"
CHEMIN_MODELE = HDFS + "/data/models/modele_fraude"


# --- La « fiche de renseignements » d'un sinistre.
# Format : nom de colonne -> (libellé affiché, valeur par défaut du simulateur)
# ATTENTION : l'ordre doit être le même que dans entrainer_modele_fraude.py.
CHAMPS = {
    "montant_estime": ("Montant estimé du sinistre (€)", 1000.0),
    "prime_annuelle": ("Prime annuelle (€)", 500.0),
    "jours_avant_sinistre": ("Ecart entre début contrat et sinistre", 30.0),
    "nb_paiements": ("Nbr de paiements du contrat", 10.0),
    "nb_paiements_echoues": ("Dont paiements échoués", 0.0),
}
COLONNES = list(CHAMPS)  # les noms de colonnes, dans l'ordre
 
st.set_page_config(page_title="Détection de fraude", layout="wide")

# ------------------------------------------------------------------
# Spark et modèle
# @st.cache_resource = on les mets en cache pour ne pas les recharger à chaque interaction avec le simulateur. Sinon, ça prend 10 secondes à chaque fois.
# ------------------------------------------------------------------
@st.cache_resource
def demarrer_spark():
    return (
        SparkSession.builder.appName("DashboardFraude")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .getOrCreate()
    )

@st.cache_resource(show_spinner="Chargement du modèle depuis Hadoop...")
def charger_modele():
    demarrer_spark()  # Spark doit être démarré avant de charger le modèle
    return PipelineModel.load(CHEMIN_MODELE)

# @st.cache_data = on mémorise le résultat de la lecture des prédictions pour ne pas relire Hadoop à chaque interaction avec le simulateur. Sinon, ça prend 10 secondes à chaque fois.
@st.cache_data(show_spinner="Lecture des prédictions dans Hadoop...")
def lire_resultats():
    spark = demarrer_spark()
    
    def lire_parquet(nom):
        """Lit un fichier Parquet dans Hadoop et le convertit en DataFrame Pandas."""
        df = spark.read.parquet(f"{DOSSIER}/{nom}")
        return pd.DataFrame([ligne.asDict() for ligne in df.collect()], columns=df.columns)  # conversion en Pandas pour l'affichage dans Streamlit
    
    return lire_parquet("predictions_fraude"), lire_parquet("metriques_fraude")

# ------------------------------------------------------------------
# L'affichage
# ------------------------------------------------------------------

st.title("Détection de fraude")
st.caption("Modèle Spark ML (arbre de décision) entraîné sur les sinistres AssurePlus "
           "dont le score de fraude est connu, appliqué aux sinistres sans score.")

# Bouton « tout recharger » : on vide les mémoires (caches) pour repartir à neuf
if st.sidebar.button("Recharger depuis Hadoop"):
    st.cache_data.clear()
    st.cache_resource.clear()
    
# Si Hadoop ne répond pas, on affiche un message clair et on arrête la page
try:
    predictions, metriques = lire_resultats()
    modele = charger_modele()
except Exception as erreur:
    st.error(
        "Impossible de lire le modèle ou les prédictions dans Hadoop. Lance d'abord "
        "src/pipeline/preparer_dataset_fraude.py puis src/pipeline/entrainer_modele_fraude.py."
    )
    st.exception(erreur)
    st.stop()
    
# On affiche les métriques du modèle (précision et rappel pour les sinistres suspects)
m = metriques.iloc[0]
st.subheader("Qualité du modèle")
cartes = [
    ("Sinistres d'entraînement", int(m["nb_lignes"])),
    ("dont suspects", int(m["nb_suspects"])),
    ("Précision (suspects)", f"{m['precision_suspects']:.0%}"),
    ("Rappel (suspects)", f"{m['rappel_suspects']:.0%}"),
]

# 4 colonnes, 4 cartes : on les associe une à une avec zip
for colonne, (titre, valeur) in zip(st.columns(4), cartes):
    colonne.metric(titre, valeur)
st.warning(
    "Le jeu d'entraînement est très petit : ces chiffres sont indicatifs. "
    "Les probabilités ci-dessous sont des alertes à vérifier, pas des certitudes."
)

# --- Sinistres à surveiller
st.subheader("Sinistres à surveiller")
seuil = st.slider("Afficher les sinistres dont la probabilité de fraude est au moins de", 0.0, 1.0, 0.5, 0.05)
# On filtre les sinistres dont la probabilité de fraude est au moins égale au seuil choisi par l'utilisateur.
a_surveiller = (predictions[predictions["proba_fraude"] >= seuil]
                .sort_values("proba_fraude", ascending=False))
# On affiche le nombre de sinistres signalés sur le nombre total de sinistres.
st.metric("Sinistres signalés", f"{len(a_surveiller)} sur {len(predictions)}")

# Si aucun sinistre n'atteint le seuil, on affiche un message. Sinon, on affiche la liste et un bouton pour la télécharger.
if a_surveiller.empty:
    st.info("Aucun sinistre n'atteint ce seuil.")
else:
    st.dataframe(a_surveiller, hide_index=True)
    st.download_button(
        label="Télécharger la liste (CSV)",
        # utf-8-sig + séparateur « ; » = s'ouvre correctement dans Excel en français
        data=a_surveiller.to_csv(index=False, sep=";").encode("utf-8-sig"),
        file_name="sinistres_a_surveiller.csv",
        mime="text/csv",
    )
    
# --- Ce que le modèle a appris : sur quoi l'agent se base pour décider
st.subheader("Ce que le modèle a appris")
arbre = modele.stages[-1]  # la dernière étape du pipeline = l'arbre de décision
st.caption("Importance de chaque caractéristique dans les décisions de l'arbre :")
# On affiche un graphique en barres avec l'importance de chaque caractéristique (feature) dans les décisions de l'arbre.
st.bar_chart(pd.Series(arbre.featureImportances.toArray(), index=[CHAMPS[c][0] for c in COLONNES]))
with st.expander("Règles de l'arbre de décision"):
    # L'arbre parle de « feature 0, 1, 2... » : on affiche la traduction
    st.caption(", ".join(f"feature {i} = {c}" for i, c in enumerate(COLONNES)))
    st.code(arbre.toDebugString)

# --- Simulateur : on remplit la « fiche », le modèle donne son avis
st.subheader("Simuler un sinistre")
st.caption("Saisis les caractéristiques d'un sinistre pour obtenir sa probabilité de fraude.")

# Une seule boucle crée les 5 champs, répartis sur 3 colonnes
# (i % 3 = 0, 1, 2, 0, 1 : on remplit les colonnes de gauche à droite, puis on recommence)
zones = st.columns(3)
saisies = {
    nom: zones[i % 3].number_input(libelle, min_value=0.0, value=defaut)
    for i, (nom, (libelle, defaut)) in enumerate(CHAMPS.items())
}

if st.button("Évaluer ce sinistre"):
    spark = demarrer_spark()
    # Le dictionnaire garde l'ordre des champs : on peut donc le transformer directement en une ligne de données pour le modèle.
    ligne = spark.createDataFrame([tuple(saisies.values())], COLONNES)
    # "probability" contient [proba pas fraude, proba fraude] : on prend la case 1
    resultat = modele.transform(ligne).withColumn("p", vector_to_array("probability")[1]).first()
    st.metric("Probabilité de fraude", f"{resultat['p']:.0%}")
    
