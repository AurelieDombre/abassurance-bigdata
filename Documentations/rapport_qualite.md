# Rapport de qualité : abassurance-bigdata

Date de l'analyse : 12/10/2026
Outil : SonarQube Community (analyse locale)

## 1. Verdict global

Quality Gate : [Passed / Failed]

## 2. Indicateurs SonarQube

| Indicateur | Première analyse | Dernière analyse |
| --- | --- | --- |
| Lignes de code | ~1 700 | 0 |
| Sécurité (issues) | 51 | 0 |
| Fiabilité (issues) | 1 | 0 |
| Maintenabilité (issues) | 8 | 0 |
| Couverture, ensemble du projet | 0 % | 61 % |
| Couverture, nouveau code | 0 % | 55,1 % |

**Fichiers exclus du calcul de couverture.** Les fichiers qui démarrent Spark, lisent Hadoop ou Kafka, ou ne font qu'afficher des pages Streamlit (`app/app.py`, `app/pages/`, `streaming_topics/`, `src/prediction/`, `generate_dataset.py`, `tools/`) sont exclus du calcul de couverture (`sonar.coverage.exclusions`). Ils restent analysés par SonarQube pour la qualité. Leur logique de calcul a été déplacée dans des fichiers testés. Avant cette exclusion, la couverture était de 27,4 % sur l'ensemble du projet et de 47,1 % sur le nouveau code. Détails : journal technique, US 7.4.

## 3. Tests unitaires

| Fichier testé | Tests | Où ils tournent | Résultat | Couverture |
| --- | --- | --- | --- | --- |
| `app/outils.py` | 40 | Poste | tous réussis | 100 % |
| `app/outils_perf.py` | 6 | Poste | tous réussis | 100 % |
| `app/outils_auth.py` | 17 | Poste | tous réussis | 100 % |
| `en_ligne/app.py` (et `app/auth.py`) | 20 | Poste | tous réussis | 82 % (`auth.py` : 83 %) |
| `src/pipeline/nettoyage_donnees_ia.py` | 13 | Conteneur (Spark) | tous réussis | 100 % |
| `src/pipeline/preparer_dataset_fraude.py` | 11 | Conteneur (Spark) | tous réussis | 68 % |

Commande pour tout relancer : `python lancer_tests.py`

## 4. Points d'attention

* **Parties non couvertes, et pourquoi.** Dans `preparer_dataset_fraude.py`, les fonctions qui démarrent Spark et lisent HDFS. Dans `en_ligne/app.py`, les cas où les services répondent (les tests tournent sans Kafka, Hadoop ni Spark). Ces parties se vérifient en conditions réelles, dans la version complète.
* **Bug identifié et corrigé** : contrats à NULL dans HDFS (US 8.1 et 8.2).
* **Bug potentiel documenté** : une date vide écarte une ligne sans message dans `lignes_coherentes` ; le programme affiche désormais le nombre de lignes écartées.

## 5. Conclusion

La logique de l'application (outils, authentification, version en ligne) et celle du pipeline (préparation et nettoyage des données) sont couvertes par des tests unitaires, avec au moins 68 % de couverture par fichier testé. La commande `python lancer_tests.py` relance les tests, mesure la couverture et met à jour SonarQube en une seule fois. Il reste à tester les fonctions de l'entraînement du modèle qui ne dépendent pas de HDFS, et à déclencher cette chaîne automatiquement à chaque `git push` (intégration continue).
