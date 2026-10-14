# Mapping des données

Ce document décrit, table par table, comment les colonnes des deux bases sources (AbAssurance sous Oracle, AssurePlus sous SQL Server) correspondent aux colonnes du modèle cible commun. Chaque tableau indique aussi ce qui est réellement fait dans les données fusionnées.

## Table CLIENT (`AB_CLIENT` ↔ `AP_USERS`)

| Base AbAssurance | Base AssurePlus | Modèle cible | Remarque |
| --- | --- | --- | --- |
| `AB_CLIENT_ID` | `AP_USER_ID` | `client_id` | Un préfixe `AB-` ou `AP-` est ajouté devant l'identifiant source (par exemple `AB-1`, `AP-1`), car les deux numérotations commencent à 1 et entreraient en conflit. Le champ est donc un texte. |
| `AB_NOM` + `AB_PRENOM` | `AP_FULL_NAME` | `nom_prenom` | Un seul champ dans le modèle cible. AbAssurance : le nom et le prénom sont réunis. AssurePlus : le nom complet est repris tel quel (pas de découpage, `AP_FULL_NAME` ne permet pas de séparer nom et prénom de façon fiable). |
| `AB_DATE_NAISSANCE` | `AP_BIRTH_DATE` | `date_naissance` | ⚠️ `AP_BIRTH_DATE` est stocké en texte côté source, au format `jj/mm/aaaa` (par exemple `06/10/1943`). Il est normalisé en `aaaa-mm-jj` par la routine Talaxie `normaliserDate`. |
| `AB_EMAIL` | `AP_MAIL_ADDRESS` | `email` | Nettoyé par la routine `nettoyerEmail` (minuscules, correction de `_at_`, valeur invalide devenue `NULL`). |
| `AB_TELEPHONE` | `AP_PHONE_NUMBER` | `telephone` | Valeur vide devenue `NULL`. |
| `AB_ADRESSE` | `AP_STREET_ADDRESS` | `adresse` | |
| `AB_CODE_POSTAL` | `AP_ZIP_CODE` | `code_postal` | |
| `AB_NUM_FISCAL` | `—` | `num_fiscal` | Spécifique AbAssurance : reste vide (`NULL`) pour les clients venant d'AssurePlus. |
| `AB_DATE_CREATION` | `AP_CREATED_AT` | `date_creation` | Format `aaaa-mm-jj hh:mm:ss`. |
| `AB_STATUT_CLIENT` | `AP_CUSTOMER_STATUS` | `statut_client` | Valeurs à harmoniser en français : `ACTIF/INACTIF/SUSPENDU` (AbAssurance) et `ACTIVE/INACTIVE/SUSPENDED` (AssurePlus). La traduction est faite par la fonction `harmoniser_statuts` de Spark. |
| `—` | `AP_LOYALTY_SCORE` | `loyalty_score` | Spécifique AssurePlus : reste vide pour les clients historiques d'AbAssurance. |

> ⚠️ **Anomalie constatée dans les données fusionnées.** Dans le fichier `data/output/dataClean_fusion/clients` (archive du 12 octobre), les colonnes `statut_client` et `date_creation` sont **inversées pour les 100 clients AssurePlus** : `statut_client` contient la date de création (par exemple `2023-08-07 21:31:33`) et `date_creation` contient le statut (par exemple `ACTIVE`). Les 200 clients AbAssurance sont corrects. L'en-tête du fichier place `statut_client` avant `date_creation`, ce qui laisse penser que les deux flux ne sont pas dans le même ordre au moment de la fusion. Correctif à faire dans le job Talaxie de fusion (ordre des colonnes de la sortie AssurePlus), puis republier les clients.

## CONTRAT (`AB_CONTRAT` ↔ `AP_CONTRACTS`)

| Base AbAssurance | Base AssurePlus | Modèle cible | Remarque |
| --- | --- | --- | --- |
| `AB_POLICY_NUMBER` | `AP_CONTRACT_REF` | `contrat_id` | Référence conservée telle quelle : elle est déjà préfixée dans les sources (`AB-000001`, `AP-000001`). Aucun identifiant n'est généré. |
| `AB_CLIENT_ID` | `AP_USER_ID` | `client_id` (FK) | Clé étrangère vers `CLIENT`. Le même préfixe `AB-` ou `AP-` est ajouté, pour correspondre au `client_id` de la table CLIENT. |
| `AB_TYPE_ASSURANCE` | `—` | `type_assurance` | Spécifique AbAssurance (catégorie générique : SANTE, AUTO...). Vide pour AssurePlus dans les données fusionnées. |
| `—` | `AP_PRODUCT_CODE` | `code_produit` | Spécifique AssurePlus (code produit commercial : `AUTO_BASIC`...). Nomenclature différente, non fusionnée avec `type_assurance` dans les données fusionnées. Pour le jeu de données IA, la famille de produit doit être déduite de ce code (correctif prévu, voir `dictionnaire_dataset.md`). |
| `AB_DATE_DEBUT` | `AP_START_DATE` | `date_debut` | |
| `AB_DATE_FIN` | `AP_END_DATE` | `date_fin` | |
| `AB_PRIME_ANNUELLE` | `—` | `prime_annuelle` | Spécifique AbAssurance. Vide pour AssurePlus dans les données fusionnées. |
| `—` | `AP_MONTHLY_PREMIUM` | `prime_mensuelle` | Spécifique AssurePlus : les deux colonnes cohabitent (fréquences de facturation différentes selon la source). Pour le jeu de données IA, une prime annuelle doit être calculée (`prime_mensuelle` × 12) quand elle est vide (correctif prévu, voir `dictionnaire_dataset.md`). |
| `AB_STATUT_CONTRAT` | `AP_CONTRACT_STATE` | `statut_contrat` | Les deux sources alimentent la **même colonne** : il n'y a pas de colonne `etat_contrat` dans les données fusionnées. AbAssurance : `ACTIF/RESILIE/SUSPENDU`. AssurePlus : `ACTIVE/TERMINATED/SUSPENDED`, à traduire en français (`ACTIF/RESILIE/SUSPENDU`) : dans Talaxie (correctif de l'US 8.2) et, en sécurité, par `harmoniser_statuts` dans Spark. |
| `AB_AGENCE_ID` | `—` | `agence_id` | Spécifique AbAssurance (réseau d'agences physiques). |
| `—` | `AP_BROKER_CODE` | `code_courtier` | Spécifique AssurePlus (réseau de courtiers). |

## SINISTRE (`AB_SINISTRE` ↔ `AP_CLAIMS`)

| Base AbAssurance | Base AssurePlus | Modèle cible | Remarque |
| --- | --- | --- | --- |
| `AB_CLAIM_ID` | `AP_SINISTRE_NUM` | `sinistre_id` | Numéro d'origine conservé (entier). ⚠️ Les deux numérotations se suivent (AbAssurance de 1 à 61, AssurePlus de 62 à 98). |
| `AB_POLICY_NUMBER` | `AP_CONTRACT_REF` | `contrat_id` (FK) | Clé étrangère vers `CONTRAT`. |
| `AB_DATE_SINISTRE` | `AP_INCIDENT_DATE` | `date_sinistre` | ⚠️ `AP_INCIDENT_DATE` est stocké en texte (`VARCHAR(19)`) côté source : converti en date/horodatage par Spark (`to_timestamp`) lors de la préparation. |
| `AB_MONTANT_ESTIME` | `AP_ESTIMATED_AMOUNT` | `montant_estime` | |
| `AB_STATUT_SINISTRE` | `AP_CLAIM_STATUS` | `statut_sinistre` | Valeurs à harmoniser en français : `DECLARE/EN_COURS/CLOTURE/REJETE` (AbAssurance) et `REPORTED/IN_PROGRESS/CLOSED/REJECTED` (AssurePlus). Dans les données fusionnées, les deux séries coexistent : la traduction est faite par `harmoniser_statuts` dans Spark. |
| `AB_DESCRIPTION` | `AP_CLAIM_COMMENT` | `description` | |
| `—` | `AP_FRAUD_SCORE` | `fraud_score` | Spécifique AssurePlus : utile pour le modèle IA de détection de fraude. Reste vide pour l'historique AbAssurance. |

## PAIEMENT (`AB_PAIEMENT` ↔ `AP_PAYMENTS`)

| Base AbAssurance | Base AssurePlus | Modèle cible | Remarque |
| --- | --- | --- | --- |
| `AB_PAYMENT_ID` | `AP_PAYMENT_REF` | `paiement_id` | Identifiant d'origine conservé tel quel, stocké en texte : un entier (`1`, `2`...) côté AbAssurance, un UUID côté AssurePlus. Aucun conflit possible entre les deux sources, donc pas de préfixe. |
| `AB_POLICY_NUMBER` | `AP_CONTRACT_REF` | `contrat_id` (FK) | Clé étrangère vers `CONTRAT`. |
| `AB_DATE_PAIEMENT` | `AP_PAYMENT_DATETIME` | `date_paiement` | |
| `AB_MONTANT` | `AP_AMOUNT_PAID` | `montant` | |
| `AB_MODE_PAIEMENT` | `AP_PAYMENT_CHANNEL` | `mode_paiement` | Valeurs à harmoniser en français : `PRELEVEMENT/CARTE/VIREMENT/CHEQUE` (AbAssurance) et `CARD/BANK_TRANSFER/DIRECT_DEBIT` (AssurePlus). Correspondance prévue : `CARD` → `CARTE`, `BANK_TRANSFER` → `VIREMENT`, `DIRECT_DEBIT` → `PRELEVEMENT`. **Non appliquée pour l'instant** : les données fusionnées contiennent 7 valeurs distinctes, et `harmoniser_statuts` ne traite pas cette colonne. |
| `—` | `AP_TRANSACTION_STATUS` | `statut_transaction` | Spécifique AssurePlus : reste vide pour l'historique AbAssurance. Valeurs source `SUCCESS/PENDING/FAILED`, traduites par `harmoniser_statuts` en `REUSSI/EN_ATTENTE/ECHOUE`. |

## Résumé : où est faite chaque harmonisation

| Sujet | Où |
| --- | --- |
| Préfixes `AB-` / `AP-` des clients | Talaxie (job de fusion) |
| Formats de dates, e-mails, téléphones, valeurs vides | Talaxie (routine `DataCleaning`) |
| Traduction des statuts (client, contrat, sinistre, transaction) | Spark (`harmoniser_statuts`), et Talaxie pour les contrats (US 8.2) |
| Prime annuelle et type d'assurance pour AssurePlus | Spark (préparation du jeu de données IA) : correctif prévu, pas encore appliqué |
| Modes de paiement | Pas encore appliquée |

---
