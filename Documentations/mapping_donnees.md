# Mapping des données

## Table CLIENT (`AB_CLIENT` ↔ `AP_USERS`)

| Base AbAssurance | Base AssurePlus | Modèle cible | Remarque |
|---|---|---|---|
| `AB_CLIENT_ID` | `AP_USER_ID` | `client_id` | Nouvel identifiant technique généré (les deux ID sources ne sont pas compatibles entre elles) |
| `AB_NOM` + `AB_PRENOM` | `AP_FULL_NAME` | `nom` / `prenom` | Chez AssurePlus, il faut **splitter** `AP_FULL_NAME` en nom/prénom (règle à définir, ex. premier mot = prénom) |
| `AB_DATE_NAISSANCE` | `AP_BIRTH_DATE` | `date_naissance` | ⚠️ `AP_BIRTH_DATE` est stocké en texte libre côté source (formats potentiellement incohérents) — à normaliser en date |
| `AB_EMAIL` | `AP_MAIL_ADDRESS` | `email` | |
| `AB_TELEPHONE` | `AP_PHONE_NUMBER` | `telephone` | |
| `AB_ADRESSE` | `AP_STREET_ADDRESS` | `adresse` | |
| `AB_CODE_POSTAL` | `AP_ZIP_CODE` | `code_postal` | |
| `AB_NUM_FISCAL` | `—` | `num_fiscal` | Spécifique AbAssurance — restera vide (`NULL`) pour les clients venant d'AssurePlus |
| `AB_DATE_CREATION` | `AP_CREATED_AT` | `date_creation` | |
| `AB_STATUT_CLIENT` | `AP_CUSTOMER_STATUS` | `statut_client` | Valeurs à harmoniser : `ACTIF/INACTIF/SUSPENDU` (FR) vs `ACTIVE/INACTIVE/SUSPENDED` (EN) : Pour la cohérence ce sera en français |
| `—` | `AP_LOYALTY_SCORE` | `loyalty_score` | Spécifique AssurePlus — restera vide pour les clients historiques AbAssurance |

## CONTRAT (`AB_CONTRAT` ↔ `AP_CONTRACTS`)

| Base AbAssurance | Base AssurePlus | Modèle cible | Remarque |
|---|---|---|---|
| `AB_POLICY_NUMBER` | `AP_CONTRACT_REF` | `contrat_id` | Nouvel identifiant technique généré |
| `AB_CLIENT_ID` | `AP_USER_ID` | `client_id` (FK) | Clé étrangère vers `CLIENT` |
| `AB_TYPE_ASSURANCE` | `—` | `type_assurance` | Spécifique AbAssurance (catégorie générique : SANTE, AUTO...) |
| `—` | `AP_PRODUCT_CODE` | `code_produit` | Spécifique AssurePlus (code produit commercial : AUTO_PREMIUM...) — nomenclature différente, pas fusionnée avec `type_assurance` |
| `AB_DATE_DEBUT` | `AP_START_DATE` | `date_debut` | |
| `AB_DATE_FIN` | `AP_END_DATE` | `date_fin` | |
| `AB_PRIME_ANNUELLE` | `—` | `prime_annuelle` | Spécifique AbAssurance |
| `—` | `AP_MONTHLY_PREMIUM` | `prime_mensuelle` | Spécifique AssurePlus — les deux cohabitent (fréquences de facturation différentes selon la source) |
| `AB_STATUT_CONTRAT` | `—` | `statut_contrat` | Spécifique AbAssurance (`ACTIF/RESILIE/SUSPENDU`) |
| `—` | `AP_CONTRACT_STATE` | doubon avec `statut_contrat` | Spécifique AssurePlus (`ACTIVE/TERMINATED/SUSPENDED`) : on concerve uniquement `statut_contrat` |
| `AB_AGENCE_ID` | `—` | `agence_id` | Spécifique AbAssurance (réseau d'agences physiques) |
| `—` | `AP_BROKER_CODE` | `code_courtier` | Spécifique AssurePlus (réseau de courtiers) |

## SINISTRE (`AB_SINISTRE` ↔ `AP_CLAIMS`)

| Base AbAssurance | Base AssurePlus | Modèle cible | Remarque |
|---|---|---|---|
| `AB_CLAIM_ID` | `AP_SINISTRE_NUM` | `sinistre_id` | Nouvel identifiant technique généré |
| `AB_POLICY_NUMBER` | `AP_CONTRACT_REF` | `contrat_id` (FK) | Clé étrangère vers `CONTRAT` |
| `AB_DATE_SINISTRE` | `AP_INCIDENT_DATE` | `date_sinistre` | ⚠️ `AP_INCIDENT_DATE` est stocké en texte (VARCHAR(19)) — à convertir en type date/timestamp |
| `AB_MONTANT_ESTIME` | `AP_ESTIMATED_AMOUNT` | `montant_estime` | |
| `AB_STATUT_SINISTRE` | `AP_CLAIM_STATUS` | `statut_sinistre` | Valeurs à harmoniser : `DECLARE/EN_COURS/CLOTURE/REJETE` vs `REPORTED/IN_PROGRESS/CLOSED/REJECTED` |
| `AB_DESCRIPTION` | `AP_CLAIM_COMMENT` | `description` | |
| `—` | `AP_FRAUD_SCORE` | `fraud_score` | Spécifique AssurePlus — utile pour le futur modèle IA de détection de fraude (restera vide pour l'historique AbAssurance) |

## PAIEMENT (`AB_PAIEMENT` ↔ `AP_PAYMENTS`)

| Base AbAssurance | Base AssurePlus | Modèle cible | Remarque |
|---|---|---|---|
| `AB_PAYMENT_ID` | `AP_PAYMENT_REF` | `paiement_id` | Nouvel identifiant technique généré (`AP_PAYMENT_REF` est un UUID côté source) |
| `AB_POLICY_NUMBER` | `AP_CONTRACT_REF` | `contrat_id` (FK) | Clé étrangère vers `CONTRAT` |
| `AB_DATE_PAIEMENT` | `AP_PAYMENT_DATETIME` | `date_paiement` | |
| `AB_MONTANT` | `AP_AMOUNT_PAID` | `montant` | |
| `AB_MODE_PAIEMENT` | `AP_PAYMENT_CHANNEL` | `mode_paiement` | Valeurs à harmoniser : `PRELEVEMENT/CARTE/VIREMENT/CHEQUE` vs `CARD/BANK_TRANSFER/DIRECT_DEBIT` / Privilégier en français pour la cohérence|
| `—` | `AP_TRANSACTION_STATUS` | `statut_transaction` | Spécifique AssurePlus — restera vide pour l'historique AbAssurance |

---
