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
| Couverture, ensemble du projet | 0 % | 14,9 % |
| Couverture, nouveau code | 0 % | 69,5 % |

![dashboard_sonarqube.png](images_readme/dashboard_sonarqube.png)

## 3. Tests unitaires

| Fichier | Tests | Résultat | Couverture |
| --- | --- | --- | --- |
| `app/outils.py` | 35 | tous réussis | 100 % |
| `src/pipeline/preparer_dataset_fraude.py` | 10 | tous réussis | 64 % |

Commande pour tout relancer : `python lancer_tests.py`

## 4. Points d'attention

* Couverture globale basse : seuls les fichiers qui contiennent de la logique sont testés.
* [anomalies restantes que tu décides de ne pas corriger, avec la raison]
* Bug identifié et corrigé : contrats à NULL dans HDFS (US 8.1 et 8.2).

## 5. Conclusion

* Tests des transformations Spark (`harmoniser_statuts`, `nettoyer`) avec un mini-Spark local sur quelques lignes inventées.
* Tests d'intégration lancés dans Docker avec le pipeline complet.
* Intégration continue avec blocage de la livraison si un test échoue.
