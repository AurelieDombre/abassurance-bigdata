# lancer_tests.py : relance toute la chaîne qualité en une seule commande
# (tests -> couverture -> SonarQube).


import subprocess
import sys
from pathlib import Path

# Le dossier du projet = le dossier où se trouve ce fichier
PROJET = Path(__file__).resolve().parent

# Le nom du conteneur Docker où tournent les tests du pipeline
CONTENEUR = "pyspark-app"


def lancer(commande, secret=False):
    # Commande dans le terminal
    # Si la commande contient le token, on n'affiche pas son texte.
    print("\n>>>", "(commande masquée)" if secret else commande)
    resultat = subprocess.run(commande, shell=True, cwd=PROJET)

    # Un code différent de 0 veut dire "ça a raté" : on s'arrête là,
    # pour ne pas envoyer un rapport faux à SonarQube.
    if resultat.returncode != 0:
        print("\nERREUR : une étape a échoué, arrêt du script.")
        sys.exit(1)


def lire_token():
    # Le token est dans le fichier .env (qui n'est PAS sur GitHub).
    # On cherche la ligne qui commence par SONAR_TOKEN=
    fichier_env = PROJET / ".env"
    if not fichier_env.exists():
        print("Fichier .env introuvable : crée-le avec SONAR_TOKEN=...")
        sys.exit(1)

    for ligne in fichier_env.read_text(encoding="utf-8").splitlines():
        if ligne.startswith("SONAR_TOKEN="):
            return ligne.split("=", 1)[1].strip().strip('"')

    print("Pas de ligne SONAR_TOKEN=... dans le fichier .env")
    sys.exit(1)


# ------------------------------------------------------------------
# Étape 1 : envoyer les fichiers à jour dans le conteneur
# ------------------------------------------------------------------
token = lire_token()

# Liste des fichiers à envoyer dans le conteneur : (fichier sur le PC, destination dans Docker).
FICHIERS_A_ENVOYER = [
    (r"src\pipeline\preparer_dataset_fraude.py", "/app/src/pipeline/preparer_dataset_fraude.py"),
    (r"src\pipeline\nettoyage_donnees_ia.py", "/app/src/pipeline/nettoyage_donnees_ia.py"),
    (r"tests\test_preparer_dataset_fraude.py", "/app/tests/test_preparer_dataset_fraude.py"),
    (r"tests\test_nettoyage_donnees_ia.py", "/app/tests/test_nettoyage_donnees_ia.py"),
    # Modèle de fraude : le script et ses tests (preparer, evaluer)
    (r"src\prediction\entrainer_modele_fraude.py", "/app/src/prediction/entrainer_modele_fraude.py"),
    (r"tests\test_entrainer_modele_fraude.py", "/app/tests/test_entrainer_modele_fraude.py"),
    (".coveragerc", "/app/.coveragerc"),
]
for local, distant in FICHIERS_A_ENVOYER:
    lancer(f"docker cp {local} {CONTENEUR}:{distant}")

# ------------------------------------------------------------------
# Étape 2 : tests du pipeline, dans le conteneur (là où Spark est installé)
# ------------------------------------------------------------------
lancer(
    f'docker exec {CONTENEUR} sh -c "cd /app && PYTHONPATH=/app/src/pipeline:/app/src/prediction '
    "python -m pytest /app/tests/test_preparer_dataset_fraude.py "
    "/app/tests/test_nettoyage_donnees_ia.py "
    "/app/tests/test_entrainer_modele_fraude.py "
    "--cov=preparer_dataset_fraude --cov=nettoyage_donnees_ia "
    "--cov-report=term-missing "
    '--cov-report=xml:/app/coverage_fraude.xml -v"'
)

# ------------------------------------------------------------------
# Étape 3 : tests de app/outils.py, directement en local (pas besoin de Spark pour ça)
# ------------------------------------------------------------------
lancer(
    f'"{sys.executable}" -m pytest tests/test_outils.py tests/test_outils_perf.py '
    "tests/test_outils_auth.py tests/test_en_ligne.py tests/test_donnees_fusion.py "
    "--cov=app --cov=en_ligne --cov-report=term-missing --cov-report=xml:coverage.xml -v"
)

# ------------------------------------------------------------------
# Étape 4 : récupérer le rapport du conteneur et corriger l'adresse
# Dans le rapport, les fichiers sont rangés sous "/app", un dossier qui
# n'existe que dans Docker. On le remplace par le vrai dossier du projet.
# ------------------------------------------------------------------
lancer(rf"docker cp {CONTENEUR}:/app/coverage_fraude.xml coverage_fraude.xml")

rapport = PROJET / "coverage_fraude.xml"
texte = rapport.read_text(encoding="utf-8")
texte = texte.replace("<source>/app</source>", f"<source>{PROJET}</source>")
rapport.write_text(texte, encoding="utf-8")

# ------------------------------------------------------------------
# Étape 5 : envoyer le tout à SonarQube
# (la clé du projet vient du fichier sonar-project.properties)
# ------------------------------------------------------------------
lancer(f"pysonar --sonar-token={token}", secret=True)

print("\nTerminé. Résultats sur http://localhost:9000")