# abassurance-bigdata

Récupération de données suite fusion ABAssurance et AssurePlus, création de plateforme data via : Talend + Kafka + Hadoop + Spark + Python/IA

> 📄 Les explications détaillées (choix techniques, justifications, problèmes rencontrés et résolutions, avancement des User Stories) sont documentées dans [`journal_technique.md`](./Documentations/journal_technique.md).

## Liens et accès

| Description | Valeur |
| --- | --- |
| Application en ligne | https://abassurance-bigdata.streamlit.app/ |
| Comptes admin / user | Fournis dans le dossier d'examen (PDF), avec le lien de l'application |
| Compte user (login / mot de passe)  | Fournis dans le dossier d'examen (PDF), avec le lien de l'application |
| Version en ligne / version complète (mode démo) | [Documentations/MODE_DEMO.md](./Documentations/MODE_DEMO.md) |
| Journal technique | [Documentations/journal_technique.md](./Documentations/journal_technique.md) |
| Rapport de qualité | [Documentations/rapport_qualite.md](./Documentations/rapport_qualite.md) |

## Architecture

Fichiers CSV (simulant Oracle et SQL Server) → Talaxie (nettoyage, fusion) → Kafka → Spark Streaming → HDFS → Spark (préparation des données, modèle de fraude) → tableau de bord Streamlit

## Deux versions de l'application

| Version | Dossier | Usage |
| --- | --- | --- |
| **En ligne** | `en_ligne/` | Correction : consultable sans installation, à partir de données exportées de Hadoop. Le correcteur peut y importer ses propres fichiers CSV ou Parquet. |
| **Complète** | `app/` | Démonstration en local avec Docker : tout le pipeline Kafka → Hadoop → Spark, le simulateur de fraude, la surveillance des services et les alertes mail. |

Certaines fonctionnalités n'existent que dans la version complète, car elles ont besoin du pipeline. Le détail fonctionnalité par fonctionnalité, la préparation de la démonstration et le format des fichiers à importer sont dans [MODE_DEMO.md](./Documentations/MODE_DEMO.md).

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

## Prérequis

* Docker Desktop
* Java 21 LTS (Eclipse Adoptium / Temurin), `JAVA_HOME` correctement défini, Java 64 bits
* Python 3.14 (développement local) : le conteneur applicatif utilise Python 3.12
* Talaxie (voir plus bas) pour les jobs d'intégration
* SonarQube Server en local (voir plus bas) pour l'analyse qualité

## Installation de l'environnement Python

L'environnement local sert aux tests de `app/outils.py`, à la génération du jeu de données et à l'analyse SonarQube. Le reste tourne dans Docker.

```powershell
# 1. Créer l'environnement virtuel, puis l'ACTIVER avant toute installation
#    (sinon les paquets s'installent sur le Python du système, pas dans le projet)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Mettre à jour pip et installer les dépendances du projet
python -m pip install --upgrade pip
pip install -r Docker/requirements.txt

# 3. Outils utilisés seulement en local : générateur de données et analyse SonarQube
pip install Faker pysonar
```

Copier le fichier de réglages d'exemple, puis y écrire son token SonarQube (le fichier `.env` n'est jamais envoyé sur Git) :

```powershell
copy .env.exemple .env
```

## Authentification : créer les comptes

L'application demande une connexion. Elle a deux profils : `admin` (toutes les pages) et `user` (tableau de bord et détection de fraude). Les comptes sont dans le fichier `Docker/auth.env`, qui n'est jamais envoyé sur Git. **Sans ce fichier, l'application reste fermée.**

```powershell
# Depuis la racine du projet : fabrique la valeur de AUTH_USERS (l'outil demande
# un identifiant, un rôle et un mot de passe pour chaque compte)
python tools/generer_hash.py

# Copier le modèle, puis y coller la valeur affichée (une seule ligne)
copy Docker\auth.env.example Docker\auth.env
```

Les mots de passe sont enregistrés sous forme de hash (PBKDF2-SHA256 salé), jamais en clair. Détails : [journal technique, section Authentification](./Documentations/journal_technique.md#authentification-et-rôles-accès-admin--user).

## Lancement du projet

Les étapes se font dans cet ordre. Les commandes `docker` se lancent depuis n'importe quel dossier, sauf `docker compose` qui se lance depuis `Docker`.

### 1. Démarrer la plateforme (Kafka, Hadoop, Spark, tableau de bord)

Prérequis : le fichier `Docker/auth.env` doit exister (voir « Authentification : créer les comptes »), sinon le conteneur de l'application démarre mais l'écran de connexion affiche « Aucun compte configuré ».

```powershell
cd Docker
docker compose up -d --build

# Vérifier que tous les conteneurs sont démarrés
docker compose ps
cd ..
```

### 2. Préparer l'environnement

Ce script crée les topics Kafka et les dossiers HDFS (`/data/kafka/...`) : à faire une seule fois après le premier démarrage.

```powershell
.\Docker\init-environment.ps1
```

### 3. Générer les jeux de données de départ

Crée les fichiers CSV dans `data/ab_assurance` (AbAssurance, type Oracle) et `data/assure_plus` (AssurePlus, type SQL Server).

```powershell
python generate_dataset.py
```

### 4. Lancer les jobs Talaxie

Les jobs lisent les CSV, les nettoient, les fusionnent, puis envoient les 4 tables (clients, contrats, paiements, sinistres) dans Kafka. L'ordre des jobs et leur rôle sont décrits dans le [journal technique](./Documentations/journal_technique.md). Les jobs exportés sont dans le dossier `Export Jobs Talaxie`.

### 5. Lancer le streaming Kafka vers HDFS

Chaque script lit un topic Kafka en continu et écrit dans HDFS (`/data/kafka/<table>`). Ils tournent sans s'arrêter : un terminal par table.

```powershell
docker exec -it pyspark-app python streaming_topics/streaming_clients.py
docker exec -it pyspark-app python streaming_topics/streaming_contrats.py
docker exec -it pyspark-app python streaming_topics/streaming_paiements.py
docker exec -it pyspark-app python streaming_topics/streaming_sinistres.py
```

### 6. Préparer le jeu de données et entraîner le modèle de fraude

```powershell
# Prépare une ligne par sinistre et sépare entraînement / à prédire (écrit dans /data/clean)
docker exec -it pyspark-app python src/pipeline/preparer_dataset_fraude.py

# Entraîne le modèle (validation croisée), enregistre le modèle et les prédictions
docker exec -it pyspark-app python src/prediction/entrainer_modele_fraude.py
```

### 7. Ouvrir les services

| Service | Adresse |
| --- | --- |
| Tableau de bord Streamlit | http://localhost:8501 |
| Kafka UI (topics et messages) | http://localhost:8080 |
| Mailpit (mails d'alerte) | http://localhost:8025 |

HDFS n'est volontairement pas exposé sur le réseau (sécurité). Pour voir son contenu : `docker exec namenode hdfs dfs -ls /data`.
Se connecter avec l'un des comptes du tableau « Liens et accès » : `admin` voit les trois pages (tableau de bord, détection de fraude, état des services), `user` voit le tableau de bord et la détection de fraude.

## Vérifier que Kafka et Hadoop fonctionnent

```bash
# Vérification des conteneurs
docker compose ps

# Vérifier que Hadoop est fonctionnel
docker exec namenode hdfs dfs -ls /data
```

Créer un dossier de test dans HDFS :

```bash
docker exec -it namenode hdfs dfs -mkdir -p /abassurance/test
```

Créer et lister un topic Kafka de test :

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic test-abassurance --bootstrap-server localhost:9092
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Tests unitaires et qualité du code

Les tests de `app/outils.py` se lancent sur le PC. Les tests du pipeline (`preparer_dataset_fraude.py`) ont besoin de Spark : ils se lancent dans le conteneur `pyspark-app`.

```powershell
# Tests de la logique de l'application (directement sur le PC)
pytest tests/test_outils.py tests/test_outils_perf.py tests/test_outils_auth.py

# Chaîne complète en une commande :
# tests du pipeline dans Docker, tests de outils, couverture, puis envoi à SonarQube
python lancer_tests.py
```

Pour `lancer_tests.py`, il faut : les conteneurs démarrés, l'environnement virtuel activé, `pysonar` installé, SonarQube Server démarré et un token SonarQube dans le fichier `.env` (ligne `SONAR_TOKEN=...`).

Résultats et explications : [rapport de qualité](./Documentations/rapport_qualite.md) et [journal technique](./Documentations/journal_technique.md).

## Installation de SonarQube

**SonarQube for IDE** (extension VS Code) : affiche les problèmes de qualité directement dans l'éditeur.
**SonarQube Server** : analyse le projet et affiche le tableau de bord complet dans un navigateur.

Installation du serveur en local :

1. Télécharger le package en fonction de l'OS.
2. Aller dans le dossier `C:\sonarqube\bin\windows-x86-64`.
3. Lancer `StartSonar.bat`.
4. Ouvrir `localhost:9000` : SonarQube Server s'ouvre.

Créer le projet et l'analyser en suivant les étapes indiquées sur la page de SonarQube Server pour obtenir le rapport. Pour les analyses suivantes, créer un token (*My Account → Security*) et le mettre dans le fichier `.env`. La clé du projet et les dossiers analysés sont dans `sonar-project.properties`.

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
8. Exécuter le job : l'absence d'erreur confirme le succès de l'installation.

## Test de l'installation PySpark (optionnel)

Fichiers dans le dossier `script_test_installation`.

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r Docker/requirements.txt
python essaie-pyspark.py

```

PySpark 4.2.0 est installé avec les dépendances du projet (`Docker/requirements.txt`).

## Tableau des versions installées

| Outil | Version | Mode d'installation |
| --- | --- | --- |
| Talaxie (fork Talend Open Studio DI) | V8.9.0-SNAPSHOT | Natif Windows, JDK 21 (JDK système 25 non compatible) |
| Apache Kafka | 4.3.1 | Docker, image `apache/kafka:latest`, mode KRaft (sans Zookeeper) |
| Apache Hadoop | 3.2.1 | Docker, images `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` et `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` |
| Apache Spark (PySpark) | 4.2.0 | Docker, image de base `python:3.12-slim` + OpenJDK 21 |
| Python (environnement de dev local) | 3.14.5 | venv natif Windows |
| Python (conteneur applicatif) | 3.12 | Docker, choisi pour sa compatibilité éprouvée avec PySpark en environnement Linux conteneurisé |