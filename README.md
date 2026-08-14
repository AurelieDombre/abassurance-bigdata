# abassurance-bigdata
Récupération de données suite fusion ABAssurance et AssurPlus, création de plateforme data via : Talend + Kafka + Hadoop + Spark + Python/IA


## Convention de nommage des branches

Le projet utilise une convention de nommage inspirée de Git Flow afin de faciliter l'organisation du développement et d'identifier rapidement l'objectif de chaque branche.

| Type               | Convention                   | Exemple                         |
| ------------------ | ---------------------------- | ------------------------------- |
| Branche principale | `main`                       | `main`                          |
| Développement      | `develop`                    | `develop`                       |
| Fonctionnalité     | `feature/<id>-<description>` | `feature/US1.1-mapping-donnees` |
| Correction         | `bugfix/<id>-<description>`  | `bugfix/US8.1-erreur-pipeline`  |
| Correction urgente | `hotfix/<description>`       | `hotfix/erreur-kafka`           |
| Version            | `release/<version>`          | `release/1.0.0`                 |


Règles :

* Les noms de branches sont écrits en minuscules.
* Les mots sont séparés par des tirets.
* Les espaces et caractères spéciaux sont interdits.
* Les branches feature et bugfix sont associées à une User Story lorsque cela est possible.
* Les nouvelles branches sont créées à partir de develop.
* Les fonctionnalités terminées sont fusionnées dans develop.
* main contient uniquement du code validé et stable.


## Environnement

Création de l'environnement python avec la commande ```python -m venv .venv``` sur le powerShell
Mise à jour de pip : ```python -m pip install --upgrade pip```
Activation de l'environnement : ```.\.venv\Scripts\Activate.ps1```
Fichier requirements.txt généré : ````pip freeze > requirements.txt````

### Choix du framework Python

Le projet utilise PySpark comme framework Python principal.

PySpark a été choisi car il permet d'utiliser Apache Spark
depuis Python pour effectuer des traitements distribués sur
de grands volumes de données.

Il est particulièrement adapté à l'architecture du projet,
qui repose sur Hadoop pour le stockage et Spark pour le
traitement des données.

La version de python 3.14 est supporté par PySpark 4.2.0 et donc Apache Spark.

Installation de la version 4.2.0 de pySpark : ````pip install pyspark==4.2.0````

## Installation & exécution de pyspark 
Test de pySpark dans un container Docker afin de vérifier son installation et son exécution.

### Prérequis
- Docker Desktop

### Lancer le projet
A partir du dossier Docker

```bash
docker build -t abassurance-pyspark .
docker run --rm abassurance-pyspark
```

### Développement local (optionnel)
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python essaie-pyspark.py
```
