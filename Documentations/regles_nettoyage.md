# Règles de nettoyage des données

Ce document formalise les règles de nettoyage appliquées aux données extraites d'AbAssurance (Oracle) et AssurePlus (SQL Server), avant leur intégration dans le modèle cible commun.

Il répond aux critères d'acceptation de l'US2.1 :

- Règles de nettoyage définies et appliquées
- Rapport de qualité des données généré (taux d'anomalies avant/après)
- Aucune donnée obligatoire manquante sur les enregistrements validés

## Principes généraux

- Une ligne est **rejetée** (exclue du fichier nettoyé, tracée dans un fichier `_rejets`) si elle viole une règle portant sur un champ **obligatoire**.
- Une ligne est **corrigée** (valeur reformatée ou remplacée) si elle viole une règle portant sur un champ **optionnel**, ou si l'anomalie est automatiquement réparable (format de date, email malformé connu).
- Aucune correction n'est appliquée silencieusement : chaque anomalie détectée (corrigée ou rejetée) est comptabilisée dans le rapport de qualité (voir section finale).
- Les doublons intra-table sont détectés sur la clé primaire de chaque table (voir US2.1) ; les doublons inter-systèmes (même client chez AbAssurance et AssurePlus) sont traités séparément en US2.2, une fois les deux schémas harmonisés.

## AbAssurance (Oracle)

### AB_CLIENT

| Champ | Obligatoire | Règle de nettoyage |
| --- | --- | --- |
| ab_client_id | Oui | Unicité (tUniqRow) ; rejet si dupliqué |
| ab_nom | Oui | Rejet si vide |
| ab_prenom | Oui | Rejet si vide |
| ab_date_naissance | Oui | Rejet si non-parsable ; normalisation vers `yyyy-MM-dd` sinon |
| ab_email | Non | Vide → `NON_RENSEIGNE` ; `_at_` détecté → remplacé par `@` ; toujours invalide après correction → flag anomalie non-bloquante |
| ab_telephone | Non | Vide → `NON_RENSEIGNE` ; normalisation en supprimant espaces/points/tirets/préfixe `+33`/`(0)` |
| ab_adresse | Non | Vide → `NON_RENSEIGNE` |
| ab_code_postal | Non | Doit faire 5 caractères numériques ; sinon flag anomalie non-bloquante |
| ab_num_fiscal | Oui | Rejet si vide |
| ab_date_creation | Oui | Rejet si non-parsable ; normalisation vers `yyyy-MM-dd HH:mm:ss` sinon |
| ab_statut_client | Oui | Doit être `ACTIF`, `INACTIF` ou `SUSPENDU` ; sinon flag anomalie |

### AB_CONTRAT

| Champ | Obligatoire | Règle de nettoyage |
| --- | --- | --- |
| ab_policy_number | Oui | Unicité ; rejet si dupliqué |
| ab_client_id | Oui | Rejet si vide (clé étrangère obligatoire) |
| ab_type_assurance | Oui | Rejet si vide |
| ab_date_debut, ab_date_fin | Oui | Rejet si non-parsable ; normalisation `yyyy-MM-dd` ; incohérence si date_fin < date_debut → flag anomalie |
| ab_prime_annuelle | Oui | Rejet si vide ou non-numérique ou négative |
| ab_statut_contrat | Oui | Valeur attendue à documenter selon les valeurs réellement observées dans le jeu de données |
| ab_agence_id | Non | Vide → `NON_RENSEIGNE` |

### AB_SINISTRE

| Champ | Obligatoire | Règle de nettoyage |
| --- | --- | --- |
| ab_claim_id | Oui | Unicité ; rejet si dupliqué |
| ab_policy_number | Oui | Rejet si vide (clé étrangère obligatoire) |
| ab_date_sinistre | Oui | Rejet si non-parsable ; normalisation `yyyy-MM-dd HH:mm:ss` |
| ab_montant_estime | Oui | Rejet si vide, non-numérique ou négatif |
| ab_statut_sinistre | Oui | Flag anomalie si valeur hors liste attendue |
| ab_description | Non | Vide → `NON_RENSEIGNE` |

### AB_PAIEMENT

| Champ | Obligatoire | Règle de nettoyage |
| --- | --- | --- |
| ab_payment_id | Oui | Unicité ; rejet si dupliqué |
| ab_policy_number | Oui | Rejet si vide (clé étrangère obligatoire) |
| ab_date_paiement | Oui | Rejet si non-parsable ; normalisation `yyyy-MM-dd` |
| ab_montant | Oui | Rejet si vide, non-numérique ou négatif |
| ab_mode_paiement | Non | Vide → `NON_RENSEIGNE` |

## AssurePlus (SQL Server)

### AP_USERS

| Champ | Obligatoire | Règle de nettoyage |
| --- | --- | --- |
| ap_user_id | Oui | Unicité ; rejet si dupliqué |
| ap_full_name | Oui | Rejet si vide |
| ap_birth_date | Oui | Rejet si non-parsable ; normalisation `yyyy-MM-dd` (formats source observés : `yyyy-MM-dd` et `dd/MM/yyyy`) |
| ap_mail_address | Non | Vide → `NON_RENSEIGNE` ; `_at_` détecté → remplacé par `@` |
| ap_phone_number | Non | Vide → `NON_RENSEIGNE` ; normalisation identique à ab_telephone |
| ap_street_address | Non | Vide → `NON_RENSEIGNE` |
| ap_zip_code | Non | Doit faire 5 caractères numériques ; sinon flag anomalie non-bloquante |
| ap_created_at | Oui | Rejet si non-parsable ; normalisation `yyyy-MM-dd HH:mm:ss` |
| ap_customer_status | Oui | Doit être `ACTIVE`, `INACTIVE` ou `SUSPENDED` ; sinon flag anomalie |
| ap_loyalty_score | Non | Vide ou non-numérique → flag anomalie non-bloquante |

### AP_CONTRACTS

| Champ | Obligatoire | Règle de nettoyage |
| --- | --- | --- |
| ap_contract_ref | Oui | Unicité ; rejet si dupliqué |
| ap_user_id | Oui | Rejet si vide (clé étrangère obligatoire) |
| ap_product_code | Oui | Rejet si vide |
| ap_start_date, ap_end_date | Oui | Rejet si non-parsable ; normalisation `yyyy-MM-dd HH:mm:ss` ; incohérence si end_date < start_date → flag anomalie |
| ap_monthly_premium | Oui | Rejet si vide, non-numérique ou négatif |
| ap_contract_state | Oui | Flag anomalie si valeur hors liste attendue |
| ap_broker_code | Non | Vide → `NON_RENSEIGNE` |

### AP_CLAIMS

| Champ | Obligatoire | Règle de nettoyage |
| --- | --- | --- |
| ap_sinistre_num | Oui | Unicité ; rejet si dupliqué |
| ap_contract_ref | Oui | Rejet si vide (clé étrangère obligatoire) |
| ap_incident_date | Oui | Rejet si non-parsable ; normalisation `yyyy-MM-dd` |
| ap_estimated_amount | Oui | Rejet si vide, non-numérique ou négatif |
| ap_claim_status | Oui | Flag anomalie si valeur hors liste attendue |
| ap_claim_comment | Non | Vide → `NON_RENSEIGNE` |
| ap_fraud_score | Non | Vide ou non-numérique → flag anomalie non-bloquante |

### AP_PAYMENTS

| Champ | Obligatoire | Règle de nettoyage |
| --- | --- | --- |
| ap_payment_ref | Oui | Unicité ; rejet si dupliqué |
| ap_contract_ref | Oui | Rejet si vide (clé étrangère obligatoire) |
| ap_payment_datetime | Oui | Rejet si non-parsable ; normalisation `yyyy-MM-dd HH:mm:ss` |
| ap_amount_paid | Oui | Rejet si vide, non-numérique ou négatif |
| ap_payment_channel | Non | Vide → `NON_RENSEIGNE` |
| ap_transaction_status | Oui | Flag anomalie si valeur hors liste attendue |

## Rapport de qualité des données

Chaque exécution d'un job de nettoyage génère une ligne dans `data/logs/rapport_qualite.csv`, comparant le taux d'anomalies avant et après nettoyage :

```csv
date_execution,table,nb_lignes_total,nb_anomalies_avant,taux_anomalies_avant,nb_lignes_rejetees,nb_anomalies_apres,taux_anomalies_apres
```

- `nb_anomalies_avant` : nombre de lignes du fichier extrait violant au moins une règle du tableau ci-dessus.
- `nb_lignes_rejetees` : nombre de lignes exclues du fichier nettoyé (violation d'une règle sur un champ obligatoire).
- `nb_anomalies_apres` : nombre de lignes restantes (non rejetées) présentant encore une anomalie non-bloquante après correction (ex : email toujours invalide malgré la tentative de correction).
- `taux_anomalies_*` : `nb_anomalies / nb_lignes_total`, exprimé en pourcentage.
