# Rapport de qualité : abassurance-bigdata

Date de l'analyse : [JJ/MM/2026 — date de la capture ci-dessous]
Outil : SonarQube Community (analyse locale) + intégration continue GitHub Actions

## 1. Verdict global

**Quality Gate :  Failed**

La Quality Gate « Sonar way » échoue sur une seule condition : la couverture du *nouveau code* (55.1%) reste sous le seuil de 80 % exigé par défaut. Les conditions de fiabilité, sécurité et maintenabilité sont respectées, et la couverture de l'ensemble du projet est de **61%**. Choix assumé : les fichiers non couverts dépendent de HDFS, Kafka ou de l'affichage Streamlit ; ils se vérifient en conditions réelles (version Docker complète).

Les tests sont désormais relancés automatiquement à chaque `git push` par GitHub Actions (section 4).

![Tableau de bord SonarQube : verdict de la Quality Gate et couverture 61 %](images/sonarqube_quality_gate_final.png)

## 2. Indicateurs SonarQube

| Indicateur | Première analyse | Dernière analyse |
| --- | --- | --- |
| Lignes de code | ~1 700 | [à relever sur la capture] |
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
| `src/pipeline/preparer_dataset_fraude.py` | 11 | Conteneur (Spark) + CI | tous réussis | 68 % |
| `src/prediction/entrainer_modele_fraude.py` (`preparer`, `construire_pipeline`, `evaluer`) | 5 | Conteneur (Spark) + CI | tous réussis | hors calcul (*) |
| Données fusionnées `dataClean_fusion/clients` et `en_ligne/donnees/clients` | 3 | Poste + CI | tous réussis | contrôle de données |

(*) `src/prediction/` reste exclu du calcul de couverture : son bloc principal lit et écrit dans HDFS. Les fonctions testées (`preparer`, `evaluer`) sont celles qui portent la logique du modèle ; ajouter ces tests ne change donc pas le pourcentage de 61 %.

Les tests Spark tournent dans le conteneur `pyspark-app` en local, et sur la machine GitHub Actions (où Java 21 est installé). Ils sont sautés seulement sur un poste sans Java.

Commande pour tout relancer en local : `python lancer_tests.py`

## 4. Intégration continue (GitHub Actions)

Fichier : `.github/workflows/tests.yml`. À chaque `git push` (toutes branches) et à chaque pull request :

1. GitHub prépare une machine Linux neuve avec Python 3.12 et Java 21 (les mêmes versions que le conteneur) ;
2. installe `Docker/requirements.txt` ;
3. lance **tous** les tests avec `pytest` et mesure la couverture (`pytest-cov`) ;
4. affiche le résumé de couverture dans l'onglet *Actions* et garde `coverage.xml` et `rapport_tests.xml` en téléchargement.

Si un test échoue, le commit est marqué d'une croix rouge. Le contrôle de données (`test_donnees_fusion.py`) a été écrit pour l'anomalie « inversion statut_client / date_creation » : il échouait sur les 100 clients AssurePlus avant la correction du job Talaxie et passe après.

**Pourquoi la CI n'affiche pas 61 %.** La CI ne mesure que les fichiers qu'elle reçoit dans `--cov` (`app`, `en_ligne`, `src`), avec l'outil `coverage.py`. SonarQube calcule sa couverture sur tous les fichiers sources qu'il analyse, avec sa propre méthode (lignes et conditions), et un fichier sans rapport de couverture compte pour 0 %. Les deux chiffres n'ont donc pas le même périmètre : [expliquer ici quel(s) fichier(s) tire(nt) le chiffre SonarQube vers le bas, d'après l'onglet Measures > Coverage]. Le chiffre de référence du rapport reste celui de SonarQube ; la CI sert d'alarme automatique (tests verts ou rouges à chaque push).

![resultat_apres_ajout_test.png](images/resultat_apres_ajout_test.png)

## 5. Points d'attention

* **Parties non couvertes, et pourquoi.** Dans `preparer_dataset_fraude.py`, les fonctions qui démarrent Spark et lisent HDFS. Dans `en_ligne/app.py`, les cas où les services répondent (les tests tournent sans Kafka, Hadoop ni Spark). Ces parties se vérifient en conditions réelles, dans la version complète.
* **Bug identifié et corrigé** : contrats à NULL dans HDFS (US 8.1 et 8.2).
* **Bug corrigé après le retour du correcteur** : inversion `statut_client` / `date_creation` pour les clients AssurePlus. Cause : dans le job `transformation_client`, le schéma d'entrée AbAssurance déclarait `AB_DATE_CREATION` avant `AB_STATUT_CLIENT`, alors que le fichier nettoyé les contient dans l'ordre inverse ; Talaxie lit les colonnes par position. L'en-tête du fichier fusionné avait ensuite été modifié à la main pour remettre AbAssurance d'aplomb, ce qui a inversé AssurePlus. Correction faite dans le job (schéma d'entrée) et dans le job de publication `kafka_client`, puis republication complète (topic, HDFS, export en ligne).
* **Bug potentiel documenté** : une date vide écarte une ligne sans message dans `lignes_coherentes` ; le programme affiche désormais le nombre de lignes écartées.

## 6. Conclusion

La logique de l'application (outils, authentification, version en ligne) et celle du pipeline (préparation et nettoyage des données) sont couvertes par des tests unitaires, avec au moins 68 % de couverture par fichier testé. La commande `python lancer_tests.py` relance les tests, mesure la couverture et met à jour SonarQube en une seule fois. Les fonctions de l'entraînement du modèle qui ne dépendent pas de HDFS sont maintenant testées, et la chaîne de tests se déclenche automatiquement à chaque `git push` grâce à GitHub Actions.
