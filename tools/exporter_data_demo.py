# exporter_data_demo.py — copie ce qu'il faut de HDFS et des journaux vers des fichiers, pour la version en ligne.
#
# À lancer DANS le conteneur de l'application (il a Spark et accès à HDFS) :
#   docker cp tools/exporter_data_demo.py pyspark-app:/tmp/exporter_data_demo.py
#   docker exec -u root pyspark-app python /tmp/exporter_data_demo.py
# Résultat : data/export_demo/ (à déplacer dans en_ligne/donnees) avec :
#   clients, contrats, paiements, sinistres, predictions_fraude, metriques_fraude   (dossiers Parquet)
#   modele_fraude.json   (l'arbre de décision, pour le simulateur)
#   etat_services.txt, performances.csv   (journaux de l'état des services)
#
# Pas d'email, téléphone, adresse, numéro fiscal ni texte libre de description dans l'export (US 4.3).

import json
import os
import shutil

HDFS = "hdfs://namenode:9000/data"
SORTIE = "/data/export_demo"
JOURNAUX = {"/app/logs/etat_services.log": "etat_services.txt", "/app/logs/performances.csv": "performances.csv"}

# (sous-dossier HDFS, table, colonnes gardées ; None = toutes)
TABLES = [
    ("kafka", "clients", ["client_id", "code_postal", "date_creation", "statut_client"]),
    ("kafka", "contrats", None),
    ("kafka", "paiements", None),
    ("kafka", "sinistres", ["sinistre_id", "contrat_id", "date_sinistre", "montant_estime", "statut_sinistre"]),
    ("clean", "predictions_fraude", None),
    ("clean", "metriques_fraude", None),
]
COLONNES_MODELE = ["montant_estime", "prime_annuelle", "jours_avant_sinistre", "nb_paiements", "nb_paiements_echoues"]


def copier_table(spark, sous_dossier, nom, colonnes):
    df = spark.read.parquet(HDFS + "/" + sous_dossier + "/" + nom)
    if colonnes:
        df = df.select(*colonnes)
    # coalesce(1) : un seul fichier Parquet par table (jeu de données minuscule)
    df.coalesce(1).write.mode("overwrite").parquet("file://" + SORTIE + "/" + nom)
    print(nom + " : " + str(df.count()) + " lignes")


def proba_arbre(noeuds, valeurs):
    """Même calcul que proba_fraude() dans en_ligne/app.py (copié ici pour la vérification)."""
    par_id = {n["id"]: n for n in noeuds}
    noeud = par_id[0]
    while noeud["gauche"] != -1:
        noeud = par_id[noeud["gauche"] if valeurs[noeud["variable"]] <= noeud["seuil"] else noeud["droite"]]
    total = sum(noeud["stats"])
    return noeud["stats"][1] / total if total > 0 else 0.0


def exporter_modele(spark):
    """Exporte l'arbre de décision en JSON, puis VÉRIFIE qu'il redonne les probabilités calculées par Spark."""
    from pyspark.ml import PipelineModel
    chemin = HDFS + "/models/modele_fraude"
    arbre = PipelineModel.load(chemin).stages[-1]

    noeuds = []
    for ligne in spark.read.parquet(chemin + "/stages/*DecisionTreeClassifier*/data").collect():
        decoupe = ligne["split"]
        seuils = list(decoupe["leftCategoriesOrThreshold"]) if decoupe is not None else []
        noeuds.append({
            "id": int(ligne["id"]),
            "stats": [float(x) for x in ligne["impurityStats"]],
            "gauche": int(ligne["leftChild"]),
            "droite": int(ligne["rightChild"]),
            "variable": int(decoupe["featureIndex"]) if decoupe is not None else -1,
            "seuil": float(seuils[0]) if seuils else None,
        })

    # Vérification : l'arbre exporté doit donner les mêmes probabilités que les prédictions faites par Spark
    verifies = 0
    for ligne in spark.read.parquet(HDFS + "/clean/predictions_fraude").collect():
        valeurs = [float(ligne[c]) for c in COLONNES_MODELE]
        if abs(proba_arbre(noeuds, valeurs) - float(ligne["proba_fraude"])) > 1e-6:
            raise SystemExit("ERREUR : l'arbre exporté ne donne pas les mêmes résultats que Spark "
                             "(sinistre " + str(ligne["sinistre_id"]) + "). Le fichier modele_fraude.json n'est pas écrit.")
        verifies += 1
    print("modele_fraude.json : arbre vérifié sur " + str(verifies) + " sinistres (mêmes probabilités que Spark)")

    contenu = {
        "colonnes": COLONNES_MODELE,
        "importances": [float(x) for x in arbre.featureImportances.toArray()],
        "regles": arbre.toDebugString,
        "noeuds": noeuds,
    }
    with open(SORTIE + "/modele_fraude.json", "w", encoding="utf-8") as f:
        json.dump(contenu, f, ensure_ascii=False, indent=1)


def copier_journaux():
    for source, nom in JOURNAUX.items():
        if os.path.exists(source):
            shutil.copy(source, SORTIE + "/" + nom)
            print(nom + " : copié")
        else:
            print(nom + " : introuvable (" + source + "), ignoré")


def nettoyer(racine):
    """Supprime les fichiers .crc et _SUCCESS que Spark ajoute à côté des données."""
    for dossier, _, fichiers in os.walk(racine):
        for f in fichiers:
            if f.endswith(".crc") or f == "_SUCCESS":
                os.remove(os.path.join(dossier, f))


if __name__ == "__main__":
    from pyspark.sql import SparkSession   # importé ici pour que les fonctions pures restent testables sans Spark
    spark = (SparkSession.builder.appName("ExportDemo").master("local[2]")
             .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000").getOrCreate())
    os.makedirs(SORTIE, exist_ok=True)
    for sous_dossier, nom, colonnes in TABLES:
        copier_table(spark, sous_dossier, nom, colonnes)
    exporter_modele(spark)
    copier_journaux()
    nettoyer(SORTIE)
    spark.stop()
    print("Terminé : " + SORTIE + " (sur ton disque : data/export_demo)")
