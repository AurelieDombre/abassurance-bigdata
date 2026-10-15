# Dictionnaire de données — Dataset de détection de fraude

Ce document décrit les colonnes du jeu de données préparé pour l'entraînement
du modèle de détection de fraude (US 5.2), issu de la fusion
des bases AbAssurance (Oracle) et AssurePlus (SQL Server) via le pipeline
Talaxie → Kafka → Spark → Hadoop.

Le jeu de données est produit par le script `src/pipeline/preparer_dataset_fraude.py`
(fonction `garder_colonnes_utiles` pour la liste des colonnes).

## Table source : `dataset_fraude_entrainement` (HDFS, Parquet)

Une ligne par sinistre dont le score de fraude est connu (sinistres d'origine AssurePlus).

| Colonne | Type | Description | Origine |
| --- | --- | --- | --- |
| `sinistre_id` | Entier | Identifiant unique du sinistre : de 1 à 61 pour AbAssurance, de 62 à 98 pour AssurePlus | AB_SINISTRE.AB_CLAIM_ID / AP_CLAIMS.AP_SINISTRE_NUM (numéros d'AssurePlus décalés de 61) |
| `contrat_id` | Texte | Identifiant du contrat concerné par le sinistre (`AB-000001`, `AP-000001`...) | AB_CONTRAT.AB_POLICY_NUMBER / AP_CONTRACTS.AP_CONTRACT_REF |
| `montant_estime` | Décimal | Montant estimé du sinistre, en euros | AB_SINISTRE.AB_MONTANT_ESTIME / AP_CLAIMS.AP_ESTIMATED_AMOUNT |
| `statut_sinistre` | Texte | Statut du sinistre, harmonisé en français (DECLARE, EN_COURS, CLOTURE, REJETE) | Harmonisation des valeurs AB (déjà en français) et AP (REPORTED/IN_PROGRESS/CLOSED/REJECTED) |
| `type_assurance` | Texte | Famille de produit d'assurance (AUTO, HABITATION, SANTE, PROFESSIONNELLE, ASSISTANCE) | AB_CONTRAT.AB_TYPE_ASSURANCE, ou déduit du code produit AssurePlus (AP_PRODUCT_CODE) |
| `prime_annuelle` | Décimal | Montant annuel de la prime d'assurance du contrat, en euros | AB_CONTRAT.AB_PRIME_ANNUELLE, ou AP_CONTRACTS.AP_MONTHLY_PREMIUM × 12 |
| `jours_avant_sinistre` | Entier | Nombre de jours entre la date de début du contrat et la date du sinistre | Calculé (date_sinistre − date_debut) |
| `nb_paiements` | Entier | Nombre total de paiements enregistrés sur le contrat (0 si aucun) | Calculé à partir de AB_PAIEMENT / AP_PAYMENTS |
| `nb_paiements_echoues` | Entier | Nombre de paiements en échec sur le contrat (0 si aucun) | Calculé à partir du statut de transaction (ECHOUE) |
| `fraud_score` | Décimal | Score de fraude d'origine (0 à 100), fourni uniquement pour les sinistres issus d'AssurePlus | AP_CLAIMS.AP_FRAUD_SCORE |
| `est_suspect` | Entier | Cible du modèle : 1 si `fraud_score` ≥ 70, sinon 0. Colonne présente uniquement dans ce jeu d'entraînement | Calculé (seuil de 70, choisi arbitrairement) |

## Table associée : `dataset_fraude_a_predire` (HDFS, Parquet)

Mêmes colonnes que ci-dessus, **sauf `est_suspect`** qui n'existe pas. La colonne
`fraud_score` est présente mais entièrement vide : ce sont les sinistres
d'origine AbAssurance, pour lesquels aucun score de fraude n'a
jamais été calculé dans le système source. Ces lignes ne sont pas utilisées
pour l'entraînement, mais sont soumises au modèle une fois entraîné
(voir la User Story 5.3), pour illustrer son usage en production.

## Limite assumée

Le score de fraude (`fraud_score`) n'existe nativement que côté AssurePlus
(champ `AP_FRAUD_SCORE`). Les sinistres d'origine AbAssurance n'ont jamais eu
cette information dans le système source. Plutôt que d'inventer une étiquette
de substitution (par exemple à partir du statut `REJETE`, qui ne reflète pas
nécessairement une fraude), le modèle est entraîné uniquement sur le
sous-ensemble disposant d'un score réel, pour garantir une cible fiable.

Les scores de ce jeu de données sont générés aléatoirement : ils n'ont pas de lien
réel avec les autres colonnes, et le seuil de 70 qui définit `est_suspect` n'est
pas issu d'une analyse. Les limites du modèle sont détaillées dans la User Story 5.3
du journal technique.

## Règles de nettoyage appliquées en amont

- Suppression des espaces superflus et conversion des valeurs vides en `null`.
- Harmonisation des statuts (anglais → français) entre les deux systèmes sources.
- Suppression des doublons avec une clé composée, pour ne pas confondre des
  lignes venant de sources différentes : (`client_id`, `email`) pour les clients,
  (`contrat_id`, `client_id`) pour les contrats, (`paiement_id`, `contrat_id`)
  pour les paiements et (`sinistre_id`, `contrat_id`) pour les sinistres.
- Valeurs manquantes sur les compteurs de paiements et sur le montant estimé
  remplacées par 0.
- Exclusion des sinistres avec un montant estimé nul ou négatif (un montant
  manquant devient 0, donc la ligne est exclue), ou une date de sinistre
  antérieure à la date de début du contrat (incohérence de données).
- Une date de sinistre ou de début de contrat manquante ou illisible donne un
  nombre de jours vide : la ligne est également exclue. Le script affiche le
  nombre de lignes écartées par ce contrôle.
  