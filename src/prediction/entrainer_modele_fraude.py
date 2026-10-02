# entrainer_modele_fraude.py — US 5.3 : entraîner et évaluer le modèle de fraude (Spark ML)
#
# Le modèle apprend sur les sinistres dont le score de fraude est connu
# (dataset_fraude_entrainement, produit par l'US 5.2), puis donne une
# probabilité de fraude aux sinistres sans score (dataset_fraude_a_predire).
# Tout reste dans la plateforme : lecture et écriture dans Hadoop, calculs avec Spark.


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.functions import vector_to_array

DOSSIER = "hdfs://namenode:9000/data/clean"
CHEMIN_MODELE = "hdfs://namenode:9000/data/models/modele_fraude"

# Les caractéristiques (features) utilisées par le modèle : uniquement des
# nombres, communs aux deux sources (AbAssurance et AssurePlus).
COLONNES = ["montant_estime", "prime_annuelle", "jours_avant_sinistre",
            "nb_paiements", "nb_paiements_echoues"]
# On coupe les 37 lignes en 5 paquets (les 5 plis), d'environ 7 ou 8 lignes chacun. Parce que si on entraînes le modèle sur 37 sinistres 
# puis qu'on le testes sur ces mêmes 37, il peut les avoir appris par cœur.
NB_PLIS = 5

def demarrer_spark():
    return (
        SparkSession.builder.appName("EntrainementFraude")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )

def preparer(df):
    """Force le type des colonnes pour les transformer en nombres et remplace les valeurs vides par 0."""
    for colonne in COLONNES:
        df = df.withColumn(colonne, F.col(colonne).cast("double"))
    return df.fillna(0, subset=COLONNES)

def construire_pipeline():
    """Un pipeline Spark ML = étape 1 (assembler les colonnes) + étape 2 (l'arbre)."""
    #il prend les 5 colonnes numériques et les range dans une seule colonne features, qui contient le vecteur [montant, prime, jours, nb_paiements, nb_echoues].
    assembleur = VectorAssembler(inputCols=COLONNES, outputCol="features")
    #lit la colonne features et la colonne label (suspect ou non), et apprend les règles.
    arbre = DecisionTreeClassifier(featuresCol="features", labelCol="label", weightCol="poids", maxDepth=3)
    
    return Pipeline(stages=[assembleur, arbre])

def evaluer(train):
    """Validation croisée : chaque sinistre est prédit par un modèle qui ne l'a pas vu.
    Renvoie la précision et le rappel pour les sinistres suspects."""
    # On donne un numéro de pli (0 à 4) à chaque ligne. Pour qu'il y ait des suspects dans chaque pli (ils sont très peu nombreux), on numérote séparément les suspects
    # et les non-suspects (1, 2, 3...), puis on garde le reste de la division par 5 : les suspects sont ainsi répartis à égalité entre les 5 plis.
    # cache() fige ces numéros pour qu'ils ne changent pas entre deux utilisations.
    numero = Window.partitionBy("label").orderBy(F.rand(42))
    train = train.withColumn("pli", F.row_number().over(numero) % NB_PLIS).cache()
    
    vrais_positifs = 0    # suspects trouvés par le modèle
    faux_positifs = 0     # sinistres signalés à tort
    faux_negatifs = 0     # suspects que le modèle a manqués
    
    for pli in range(NB_PLIS):
        # On entraine sur 4 plis et on test sur le plis restant.
        modele = construire_pipeline().fit(train.filter(F.col("pli") != pli))
        test = modele.transform(train.filter(F.col("pli") == pli))
        
        for ligne in test.select("label", "prediction").collect():
            if ligne["label"] == 1 and ligne["prediction"] == 1:
                vrais_positifs += 1
            elif ligne["label"] == 0 and ligne["prediction"] == 1:
                faux_positifs += 1
            elif ligne["label"] == 1 and ligne["prediction"] == 0:
                faux_negatifs += 1
                
    # Précision : parmi les sinistres signalés, combien sont vraiment suspects ?
    signales = vrais_positifs + faux_positifs
    precision = vrais_positifs / signales if signales > 0 else 0.0
    # Rappel : parmi les vrais suspects, combien le modèle en a-t-il trouvés ?
    suspects = vrais_positifs + faux_negatifs
    rappel = vrais_positifs / suspects if suspects > 0 else 0.0
    
    return precision, rappel    
    

if __name__ == "__main__":
    spark = demarrer_spark()
    
    
    train = preparer(spark.read.parquet(DOSSIER + "/dataset_fraude_entrainement"))
    a_predire = preparer(spark.read.parquet(DOSSIER + "/dataset_fraude_a_predire"))
    train = train.withColumn("label", F.col("est_suspect").cast("double"))
    
    # Compter les suspects. Ils sont rares : on leur donne plus de poids pour que le modèle ne réponde pas toujours « non suspect ».
    nb_lignes = train.count()
    nb_positif = train.filter("label = 1").count()
    nb_negatif = nb_lignes - nb_positif
    print("Entraînement : ", nb_lignes, "lignes dont", nb_positif, "suspects" )
    
    # Si on n'a pas de suspects ou pas de non-suspects, on ne peut pas entraîner le modèle.
    if nb_positif == 0 or nb_negatif == 0:
        raise SystemExit("Pas assez de suspects ou de non-suspects pour entraîner le modèle. Il faut des sinistres suspects ET non suspects pour entraîner le modèle.")
    
    # On donne un poids plus grand aux suspects pour que le modèle ne réponde pas toujours « non suspect ».
    train = train.withColumn("poids", F.when(F.col("label") == 1, nb_negatif / nb_positif).otherwise(1.0))
    
    # Évaluer le modèle (validation croisée)
    precision, rappel = evaluer(train)
    print("Précision (suspects) :", round(precision, 2))
    print("Rappel (suspects)    :", round(rappel, 2))
    
    # Les métriques sont gardées dans Hadoop pour être affichées dans le dashboard. 
    # Très important : on convertit precision et rappel en float, sinon Spark les écrit en double et le dashboard ne peut pas les lire.
    spark.createDataFrame(
        [(nb_lignes, nb_positif, float(precision), float(rappel))],
        ["nb_lignes", "nb_suspects", "precision_suspects", "rappel_suspects"],
    ).write.mode("overwrite").parquet(DOSSIER + "/metriques_fraude")
    
    # Modèle final : entraîné sur toutes les lignes, puis sauvegardé dans Hadoop
    modele = construire_pipeline().fit(train)
    print("Règles apprises (feature 0 =", COLONNES[0] + ", 1 =", COLONNES[1] + ", etc.) :")
    # modele.stages[-1] est l'arbre de décision, et toDebugString donne les règles apprises.
    print(modele.stages[-1].toDebugString)
    # On sauvegarde le modèle dans Hadoop pour qu'il soit utilisé (prédire la fraude sur les sinistres sans score).
    modele.write().overwrite().save(CHEMIN_MODELE)
    print("Modèle sauvegardé :", CHEMIN_MODELE)
    
    # Prédictions sur les sinistres sans score
    predictions = (
        modele.transform(a_predire)
        .withColumn("proba_fraude", vector_to_array("probability")[1])
        .withColumn("suspect", F.col("prediction").cast("int"))
        .select("sinistre_id", "contrat_id", *COLONNES, "proba_fraude", "suspect")
    )
    # On sauvegarde les prédictions dans Hadoop pour qu'elles soient utilisées plus tard (dashboard).
    predictions.write.mode("overwrite").parquet(DOSSIER + "/predictions_fraude")
    print("Prédictions enregistrées :", predictions.count())