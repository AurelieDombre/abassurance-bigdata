# doc : https://docs.pytest.org/en/stable/how-to/fixtures.html
"""Tests de src/prediction/entrainer_modele_fraude.py, avec un vrai Spark local.

On teste les deux fonctions qui ne dépendent PAS de Hadoop :
  - preparer : met les colonnes au bon format (des nombres, pas de vide) ;
  - evaluer  : la validation croisée qui calcule la précision et le rappel.
"""

import os

import pytest

pytest.importorskip("pyspark")

# Ces tests ont besoin de Spark (donc de Java). Ils tournent :
#   - dans le conteneur Docker pyspark-app (fichier /.dockerenv présent) ;
#   - sur GitHub Actions (la variable CI vaut "true" sur leurs machines).
# Sur le PC sans Java, on les saute au lieu de les faire échouer.
pytestmark = pytest.mark.skipif(
    not (os.path.exists("/.dockerenv") or os.environ.get("CI") == "true"),
    reason="Tests Spark : à lancer dans le conteneur pyspark-app ou en CI",
)

from pyspark.sql import SparkSession 

import entrainer_modele_fraude as modele 


@pytest.fixture(scope="module")
def spark():
    # On crée une seule session Spark pour tous les tests de ce fichier.
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests_modele_fraude")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# ============================================================
# preparer : transformer les colonnes en nombres et remplacer les vides par 0
# ============================================================

def test_preparer_convertit_les_textes_en_nombres(spark):
    # Les colonnes arrivent en texte (comme quand on lit un CSV) :
    # preparer doit les transformer en nombres à virgule (double).
    df = spark.createDataFrame(
        [("S1", "1500.5", "800", "120", "4", "1")],
        "sinistre_id string, montant_estime string, prime_annuelle string, "
        "jours_avant_sinistre string, nb_paiements string, nb_paiements_echoues string",
    )

    resultat = modele.preparer(df)
    types = dict(resultat.dtypes)  # ex : {"montant_estime": "double", ...}
    ligne = resultat.collect()[0]

    for colonne in modele.COLONNES:
        assert types[colonne] == "double"
    assert ligne["montant_estime"] == 1500.5
    assert ligne["nb_paiements_echoues"] == 1.0
    # Les colonnes qui ne sont pas des features ne sont pas touchées
    assert types["sinistre_id"] == "string"


def test_preparer_remplace_les_valeurs_vides_par_zero(spark):
    # Un arbre de décision ne sait pas lire une case vide : il faut la remplacer par 0.
    df = spark.createDataFrame(
        [("S1", None, 900.0, None, 3.0, None)],
        "sinistre_id string, montant_estime double, prime_annuelle double, "
        "jours_avant_sinistre double, nb_paiements double, nb_paiements_echoues double",
    )

    ligne = modele.preparer(df).collect()[0]

    assert ligne["montant_estime"] == 0.0
    assert ligne["jours_avant_sinistre"] == 0.0
    assert ligne["nb_paiements_echoues"] == 0.0
    # Une valeur déjà présente reste la même
    assert ligne["prime_annuelle"] == 900.0


# ============================================================
# construire_pipeline : 2 étapes (assembler les colonnes, puis l'arbre)
# ============================================================

def test_construire_pipeline_contient_assembleur_puis_arbre():
    etapes = modele.construire_pipeline().getStages()

    assert len(etapes) == 2
    assert type(etapes[0]).__name__ == "VectorAssembler"
    assert type(etapes[1]).__name__ == "DecisionTreeClassifier"
    assert etapes[1].getMaxDepth() == 3


# ============================================================
# evaluer : validation croisée en 5 plis -> précision et rappel
# ============================================================

def _jeu_facile(spark):
    """20 sinistres très faciles à séparer : les 10 suspects ont un gros
    montant et des paiements échoués, les 10 autres non.
    Chaque pli (5 plis) contient donc 2 suspects et 2 non-suspects."""
    lignes = []
    for i in range(10):
        # suspects : montant élevé, 3 paiements échoués
        lignes.append((f"S{i}", 9000.0 + i, 500.0, 10.0, 4.0, 3.0, 1.0, 1.0))
        # non suspects : petit montant, aucun paiement échoué
        lignes.append((f"N{i}", 200.0 + i, 500.0, 300.0, 4.0, 0.0, 0.0, 1.0))
    return spark.createDataFrame(
        lignes,
        "sinistre_id string, montant_estime double, prime_annuelle double, "
        "jours_avant_sinistre double, nb_paiements double, "
        "nb_paiements_echoues double, label double, poids double",
    )


def test_evaluer_trouve_tous_les_suspects_sur_un_jeu_facile(spark):
    # Sur un contrôle, le modèle doit avoir 100 % partout :
    # précision = il ne signale personne à tort, rappel = il ne rate aucun suspect.
    precision, rappel = modele.evaluer(_jeu_facile(spark))

    assert precision == 1.0
    assert rappel == 1.0


def test_evaluer_renvoie_des_valeurs_entre_0_et_1(spark):
    # Jeu « brouillé » : les suspects et les non-suspects se ressemblent,
    # le modèle va forcément se tromper. On vérifie seulement que les
    # résultats restent des proportions valides (entre 0 et 1).
    lignes = []
    for i in range(10):
        lignes.append((f"S{i}", 1000.0 + i * 10, 500.0, 100.0, 4.0, 1.0, 1.0, 1.0))
        lignes.append((f"N{i}", 1005.0 + i * 10, 500.0, 100.0, 4.0, 1.0, 0.0, 1.0))
    df = spark.createDataFrame(
        lignes,
        "sinistre_id string, montant_estime double, prime_annuelle double, "
        "jours_avant_sinistre double, nb_paiements double, "
        "nb_paiements_echoues double, label double, poids double",
    )

    precision, rappel = modele.evaluer(df)

    assert 0.0 <= precision <= 1.0
    assert 0.0 <= rappel <= 1.0
