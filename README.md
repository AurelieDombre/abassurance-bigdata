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


