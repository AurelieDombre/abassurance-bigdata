# Dictionnaire de données — Dataset de détection de fraude

Ce document décrit les colonnes du jeu de données préparé pour l'entraînement
du modèle de détection de fraude (US 5.2), issu de la fusion
des bases AbAssurance (Oracle) et AssurePlus (SQL Server) via le pipeline
Talaxie → Kafka → Spark → Hadoop.

## Table source : `dataset_fraude_entrainement` (HDFS, Parquet)

| Colonne | Type | Description | Origine |
|---|---|---|---|
| `sinistre_id` | String | Identifiant unique du sinistre | AB_SINISTRE.AB_CLAIM_ID / AP_CLAIMS.AP_SINISTRE_NUM |
| `contrat_id` | String | Identifiant du contrat concerné par le sinistre | AB_CONTRAT.AB_POLICY_NUMBER / AP_CONTRACTS.AP_CONTRACT_REF |
| `client_id` | String | Identifiant du client/assuré | AB_CLIENT.AB_CLIENT_ID / AP_USERS.AP_USER_ID |
| `montant_estime` | Double | Montant estimé du sinistre, en euros | AB_SINISTRE.AB_MONTANT_ESTIME / AP_CLAIMS.AP_ESTIMATED_AMOUNT |
| `statut_sinistre` | String | Statut du sinistre, harmonisé en français (DECLARE, EN_COURS, CLOTURE, REJETE) | Harmonisation des valeurs AB (déjà en français) et AP (REPORTED/IN_PROGRESS/CLOSED/REJECTED) |
| `type_assurance` | String | Famille de produit d'assurance (AUTO, HABITATION, SANTE, PROFESSIONNELLE, ASSISTANCE) | AB_CONTRAT.AB_TYPE_ASSURANCE, ou déduit du code produit AssurePlus (AP_PRODUCT_CODE) |
| `prime_annuelle` | Double | Montant annuel de la prime d'assurance du contrat, en euros | AB_CONTRAT.AB_PRIME_ANNUELLE, ou AP_CONTRACTS.AP_MONTHLY_PREMIUM × 12 |
| `jours_entre_debut_contrat_et_sinistre` | Entier | Nombre de jours entre la date de début du contrat et la date du sinistre | Calculé (date_sinistre − date_debut) |
| `nb_paiements` | Entier | Nombre total de paiements enregistrés sur le contrat | Calculé à partir de AB_PAIEMENT / AP_PAYMENTS |
| `nb_paiements_echoues` | Entier | Nombre de paiements en échec sur le contrat | Calculé à partir du statut de transaction (ECHOUE) |
| `fraud_score` | Double | Score de fraude d'origine (0 à 100), fourni uniquement pour les sinistres issus d'AssurePlus | AP_CLAIMS.AP_FRAUD_SCORE |
| `est_suspect` | Entier (0 ou 1) | Cible (label) du modèle : 1 si `fraud_score` ≥ 70, sinon 0 | Calculé, seuil à ajuster selon la distribution réelle observée |

## Table associée : `dataset_fraude_a_predire` (HDFS, Parquet)

Mêmes colonnes que ci-dessus, **sans** `fraud_score` ni `est_suspect` : il s'agit
des sinistres d'origine AbAssurance, pour lesquels aucun score de fraude n'a
jamais été calculé dans le système source. Ces lignes ne sont pas utilisées
pour l'entraînement, mais pourraient être soumises au modèle une fois entraîné,
pour illustrer son usage en production.

## Limite assumée

Le score de fraude (`fraud_score`) n'existe nativement que côté AssurePlus
(champ `AP_FRAUD_SCORE`). Les sinistres d'origine AbAssurance n'ont jamais eu
cette information dans le système source. Plutôt que d'inventer une étiquette
de substitution (par exemple à partir du statut `REJETE`, qui ne reflète pas
nécessairement une fraude), le modèle est entraîné uniquement sur le
sous-ensemble disposant d'un score réel, pour garantir une cible fiable.

## Règles de nettoyage appliquées en amont

- Suppression des espaces superflus et conversion des valeurs vides en `null`.
- Harmonisation des statuts (anglais → français) entre les deux systèmes sources.
- Suppression des doublons par identifiant métier (`sinistre_id`, `contrat_id`, `client_id`).
- Exclusion des sinistres avec un montant estimé nul ou négatif, ou une date
  de sinistre antérieure à la date de début du contrat (incohérence de données).
- Valeurs manquantes sur les compteurs de paiements et montants remplacées par 0.