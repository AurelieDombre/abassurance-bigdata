# Etat_des_services.py — US 6.2 : surveiller chaque étape du pipeline.
# Etat de toute la chaîne : Kafka → Hadoop → Spark

import json
import os
import urllib.request
from datetime import datetime

import pandas as pd
import time
from outils_perf import noter_mesure
import streamlit as st
from pyspark.sql import SparkSession

# mails 
import smtplib
from email.message import EmailMessage
from outils import (tester_porte, interpreter_datanodes, trouver_pannes,
                    formater_ligne_log, construire_mail, detecter_ralentissement)

# Le carnet de bord : un simple fichier texte
FICHIER_LOG = "logs/etat_services.log"


# ------------------------------------------------------------------
# Une fonction par étape. Chacune répond par :
#   (True ou False, un petit message)
# ------------------------------------------------------------------
def verifier_kafka():
    # 19092 = la porte INTERNE de Kafka dans le réseau Docker
    if tester_porte("kafka", 19092):
        return True, "Kafka répond"
    return False, "Kafka ne répond pas"


def verifier_hadoop():
    # On demande si le namenode est en service.
    adresse = "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"
    try:
        reponse = urllib.request.urlopen(adresse, timeout=3)
        infos = json.loads(reponse.read())["beans"][0]   # la réponse
        vivants = infos["NumLiveDataNodes"]               # nombre de datanodes vivants
        silencieux = infos.get("NumStaleDataNodes", 0) # "silencieux" = pas de signal depuis 30 secondes : peut-être en panne.
    except Exception as erreur:
        # S'il ne répond pas, tout Hadoop est inutilisable
        return False, "namenode injoignable : " + str(erreur)
    # Les datanodes en bonne santé = vivants moins ceux qui sont silencieux
    # On a 2 datanodes et chaque donnée est copiée sur les 2 (réplication = 2)
    return interpreter_datanodes(vivants, silencieux)



def verifier_streaming():
    # On regarde si chaque table contient des fichiers
    tables = ["clients", "contrats", "paiements", "sinistres"]
    vides = []
    for table in tables:
        adresse = "http://namenode:9870/webhdfs/v1/data/kafka/" + table + "?op=LISTSTATUS"
        try:
            reponse = urllib.request.urlopen(adresse, timeout=3)
            donnees = json.loads(reponse.read())
            fichiers = donnees["FileStatuses"]["FileStatus"]
            if len(fichiers) == 0:
                vides.append(table)
        except Exception:
            vides.append(table)   # impossible de lire la table = problème

    if len(vides) == 0:
        return True, "les 4 tables contiennent des fichiers"
    return False, "table vide ou introuvable : " + ", ".join(vides)


def verifier_spark():
    # On donne un tout petit calcul à Spark : s'il répond juste, il tourne.
    try:
        spark = SparkSession.builder.master("local[2]").appName("EtatServices").getOrCreate()
        if spark.range(10).count() == 10:
            return True, "Spark répond"
        return False, "Spark donne un mauvais résultat"
    except Exception as erreur:
        return False, "Spark en panne : " + str(erreur)

def envoyer_mail(pannes):
    mail = construire_mail(pannes)
    with smtplib.SMTP("mailpit", 1025, timeout=5) as serveur:
        serveur.send_message(mail)
        
# ------------------------------------------------------------------
# On lance toutes les vérifications
# ------------------------------------------------------------------
st.title("État des services du pipeline")
st.caption("Kafka → Hadoop → Spark")

st.button("Revérifier maintenant")   # cliquer relance la page, donc les vérifications

# Une liste de (nom de l'étape, fonction qui la vérifie)
etapes = [
    ("Kafka", verifier_kafka),
    ("Hadoop", verifier_hadoop),
    ("Streaming", verifier_streaming),
    ("Spark", verifier_spark),
]

resultats = []   # on y range (nom, ok, message) pour chaque étape
for nom, fonction in etapes:
    debut = time.time()                  # top chrono
    ok, message = fonction()
    duree = time.time() - debut          # temps écoulé = arrivée - départ
    noter_mesure(nom, duree)             # on l'écrit dans le carnet de chronos
    resultats.append((nom, ok, message))


# ------------------------------------------------------------------
# On écrit dans les logs
# ------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)   # crée le dossier s'il n'existe pas
date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# "a" = on AJOUTE à la fin, on n'efface jamais l'historique
with open(FICHIER_LOG, "a", encoding="utf-8") as fichier:
    for nom, ok, message in resultats:
        fichier.write(formater_ligne_log(date, nom, ok, message) + "\n")


# ------------------------------------------------------------------
# L'alerte : une bannière rouge s'il y a au moins un problème
# ------------------------------------------------------------------
noms_en_panne = trouver_pannes(resultats)

if len(noms_en_panne) > 0:
    st.error("🚨 ALERTE : problème sur " + ", ".join(noms_en_panne))
    # Ecrit dans les logs si c'est un nouveau message d'erreur.
    if st.session_state.get("derniere_alerte") != noms_en_panne:
        try:
            envoyer_mail(noms_en_panne)
            st.session_state["derniere_alerte"] = noms_en_panne
            st.info("📧 Mail d'alerte envoyé")
        except Exception as erreur:
            st.warning("Mail non envoyé : " + str(erreur))
else:
    st.success("✅ Tout fonctionne")
    st.session_state["derniere_alerte"] = []   # tout va bien : on remet le compteur à zéro


# ------------------------------------------------------------------
# Les voyants : 5 cartes côte à côte
# ------------------------------------------------------------------
colonnes = st.columns(5)

# Talend en gris : il tourne sur le PC, on ne peut pas le voir depuis Docker
colonnes[0].markdown("### ⚪ Talend")
colonnes[0].caption("Non surveillable ici (il tourne hors Docker)")

# zip() = on parcourt 2 listes en parallèle : les colonnes et les résultats
for colonne, (nom, ok, message) in zip(colonnes[1:], resultats):
    if ok:
        voyant = "🟢"
    else:
        voyant = "🔴"
    colonne.markdown("### " + voyant + " " + nom)
    colonne.caption(message)


# ------------------------------------------------------------------
# L'historique
# ------------------------------------------------------------------
with st.expander("Historique des vérifications (logs)"):
    with open(FICHIER_LOG, "r", encoding="utf-8") as fichier:
        lignes = fichier.readlines()
    # On montre les 40 dernières lignes, la plus récente en premier
    dernieres = lignes[-40:]
    dernieres.reverse()
    st.code("".join(dernieres), language="text")
    
    
# ------------------------------------------------------------------
# Les performances (US 6.3)
# ------------------------------------------------------------------
st.subheader("Performances du pipeline")

# On ouvre le "carnet de chronos" rempli à chaque vérification
mesures = pd.read_csv("logs/performances.csv")

# --- Le résumé : pour chaque étape, le plus rapide, la moyenne, le plus lent
resume = mesures.groupby("etape")["duree_s"].agg(["count", "min", "mean", "max"]).round(2)
resume.columns = ["Nb de mesures", "Minimum (s)", "Moyenne (s)", "Maximum (s)"]
st.dataframe(resume)

# --- La détection de ralentissement
# Règle : la DERNIÈRE mesure est-elle plus de 2 fois la moyenne des précédentes ?
# C'est comme un coureur qui d'habitude fait 10 minutes et en met soudain 25.
ralentissements = []
for etape in mesures["etape"].unique():
    durees = mesures[mesures["etape"] == etape]["duree_s"].tolist()

    # Il faut au moins 4 mesures pour avoir une "habitude" à comparer
    if len(durees) < 4:
        continue

    ralenti, derniere, moyenne = detecter_ralentissement(durees)
    if ralenti:
        ralentissements.append(
            etape + " : " + str(round(derniere, 1)) + " s (d'habitude " + str(round(moyenne, 1)) + " s)"
        )

    # On ignore les écarts sous 1 seconde : trop petits pour compter
    if derniere > 2 * moyenne and derniere > 1:
        ralentissements.append(
            etape + " : " + str(round(derniere, 1)) + " s (d'habitude " + str(round(moyenne, 1)) + " s)"
        )

if len(ralentissements) > 0:
    st.warning("🐢 Ralentissement détecté : " + " | ".join(ralentissements))
else:
    st.success("✅ Aucun ralentissement détecté.")

# --- Un graphique par étape : la courbe des temps dans le temps
with st.expander("Évolution des temps dans le temps"):
    mesures["date"] = pd.to_datetime(mesures["date"])
    for etape in mesures["etape"].unique():
        st.caption(etape)
        sous_tableau = mesures[mesures["etape"] == etape]
        st.line_chart(sous_tableau.set_index("date")["duree_s"])