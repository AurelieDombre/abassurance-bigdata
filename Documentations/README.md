# abassurance-bigdata

Récupération de données suite fusion ABAssurance et AssurePlus, création de plateforme data via : Talend + Kafka + Hadoop + Spark + Python/IA

> 📄 Les explications détaillées (choix techniques, justifications, problèmes rencontrés et résolutions, avancement des User Stories) sont documentées dans [`JOURNAL_TECHNIQUE.md`](./JOURNAL_TECHNIQUE.md). Ce README ne contient que les informations nécessaires à l'installation et à l'exécution du projet.

## Convention de nommage des branches

Le projet utilise une convention de nommage inspirée de Git Flow afin de faciliter l'organisation du développement et d'identifier rapidement l'objectif de chaque branche.

| Type               | Convention                   | Exemple                         |
| ------------------ | ----------------------------- | -------------------------------- |
| Branche principale | `main`                        | `main`                           |
| Développement      | `develop`                     | `develop`                        |
| Fonctionnalité     | `feature/<id>-<description>`  | `feature/US1.1-mapping-donnees`  |
| Correction         | `bugfix/<id>-<description>`   | `bugfix/US8.1-erreur-pipeline`   |
| Correction urgente | `hotfix/<description>`        | `hotfix/erreur-kafka`            |
| Version            | `release/<version>`           | `release/1.0.0`                  |

Règles :

* Les noms de branches sont écrits en minuscules.
* Les mots sont séparés par des tirets.
* Les espaces et caractères spéciaux sont interdits.
* Les branches feature et bugfix sont associées à une User Story lorsque cela est possible.
* Les nouvelles branches sont créées à partir de develop.
* Les fonctionnalités terminées sont fusionnées dans develop.
* main contient uniquement du code validé et stable.

## Prérequis

* Docker Desktop
* Java 21 LTS (Eclipse Adoptium / Temurin), `JAVA_HOME` correctement défini, Java 64 bits
* Python 3.14 (développement local) — le conteneur applicatif utilise Python 3.12

## Installation de l'environnement Python

```powershell
python -m venv .venv
python -m pip install --upgrade pip
.\.venv\Scripts\Activate.ps1
pip freeze > requirements.txt
```

## Installation de SonarQube

**SonarQube for IDE** (extension VS Code) : affiche les problèmes de qualité directement dans l'éditeur.
**SonarQube Server** : analyse le projet et affiche le tableau de bord complet dans un navigateur.

Installation du serveur en local :

1. Télécharger le package en fonction de l'OS.
2. Aller dans le dossier `C:\sonarqube\bin\windows-x86-64`.
3. Lancer `StartSonar.bat`.
4. Ouvrir `localhost:9000` — SonarQube Server s'ouvre.

Créer le projet et l'analyser en suivant les étapes indiquées sur la page de SonarQube Server pour obtenir le rapport.

## Installation de PySpark

```shell
pip install pyspark==4.2.0
```

## Lancer le projet (Docker)

Prérequis : Docker Desktop.

À partir du dossier `Docker` :

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

## Installation de Talaxie (fork de Talend Open Studio)

Talend Open Studio ayant été discontinué par Qlik le 31/01/2024, le projet utilise **Talaxie**, un fork communautaire open source (licence Apache 2.0).

Prérequis : Java 17 ou 21.

Installation :

1. Télécharger la version Windows : Talaxie DI (Data Integration).
2. Extraire le package dans un dossier dont le chemin ne contient pas de caractère spécial.
3. Ouvrir l'exécutable `TOS-DIc`.
4. Créer un workspace, puis un projet.
5. Dans job : créer un job nommé "test".
6. Dans l'onglet palette, glisser un composant `tJava` dans la fenêtre du job test.
7. Double-cliquer sur l'encart `tJava` et tester avec, par exemple : `System.out.println("Talaxie OK - environnement fonctionnel");`
8. Exécuter le job — l'absence d'erreur confirme le succès de l'installation.

## Kafka et Hadoop (Docker)

```bash
# Montage du container
docker compose up -d

# Vérification des containers
docker compose ps
```

Vérifier que Hadoop est fonctionnel : http://localhost:9870

Créer un dossier de test :

```bash
docker exec -it namenode hdfs dfs -mkdir -p /abassurance/test
```

Créer et lister un topic Kafka :

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic test-abassurance --bootstrap-server localhost:9092
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Tester la connexion Spark ↔ Hadoop :

```bash
docker compose run --rm app python test_hadoop_spark.py
```

## Génération du dataset de test

```shell
pip install Faker
pip freeze > requirements.txt
py generate_dataset.py
```

## Tableau des versions installées

| Outil | Version | Mode d'installation |
| --- | --- | --- |
| Talaxie (fork Talend Open Studio DI) | V8.9.0-SNAPSHOT | Natif Windows, JDK 21 (JDK système 25 non compatible) |
| Apache Kafka | à compléter avec `kafka-topics.sh --version` | Docker, image `apache/kafka:latest`, mode KRaft (sans Zookeeper) |
| Apache Hadoop | 3.2.1 | Docker, images `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` et `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` |
| Apache Spark (PySpark) | 4.2.0 | Docker, image de base `python:3.12-slim` + OpenJDK 21 |
| Python (environnement de dev local) | 3.14.5 | venv natif Windows |
| Python (conteneur applicatif) | 3.12 | Docker, choisi pour sa compatibilité éprouvée avec PySpark en environnement Linux conteneurisé |