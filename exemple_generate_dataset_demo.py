"""
GENERATION DE JEUX DE DONNEES DE TEST
--------------------------------------
Objectif : simuler un export des deux bases sources (AbAssurance/Oracle et
AssurePlus/SQL Server) pour pouvoir developper et demontrer le pipeline
(Kafka, Hadoop, Spark) sans avoir de vraies bases de production.

On genere volontairement quelques "defauts" realistes (doublons, valeurs
manquantes, formats incoherents) car un jeu de donnees parfait ne permettrait
pas de demontrer les bonnes pratiques de data cleaning demandees au Dossier 2.

Lancement : python generate_dataset.py
Sortie    : dossier data/ab_assurance/*.csv et data/assure_plus/*.csv
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

# On fixe la graine aleatoire (seed) : ca garantit que le script genere
# EXACTEMENT les memes donnees a chaque execution. Important pour que ton
# pipeline soit reproductible (un correcteur qui relance doit voir la meme
# chose que toi).
SEED = 42
random.seed(SEED)
fake = Faker("fr_FR")
Faker.seed(SEED)

# ---------------------------------------------------------------------------
# PARAMETRES DE VOLUME (petit volume, suffisant pour du developpement/demo)
# ---------------------------------------------------------------------------
N_AB_CLIENTS = 200
N_AP_USERS = 100
CONTRATS_PAR_CLIENT_MAX = 3
SINISTRES_TAUX = 0.25   # 25% des contrats ont au moins un sinistre
PAIEMENTS_PAR_CONTRAT_MAX = 6

OUTPUT_DIR = "data"


def write_csv(rows, path, fieldnames):
    """Ecrit une liste de dictionnaires dans un fichier CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  -> {path} ({len(rows)} lignes)")


def maybe_dirty_email(email, taux_defaut=0.03):
    """Simule des emails mal saisis ou manquants (defaut de qualite realiste)."""
    r = random.random()
    if r < taux_defaut / 2:
        return ""  # email manquant
    if r < taux_defaut:
        return email.replace("@", "_at_")  # email mal formate
    return email


def maybe_missing(value, taux_defaut=0.04):
    """Simule un champ manquant au hasard."""
    return "" if random.random() < taux_defaut else value


# ===========================================================================
# 1. BASE AbAssurance (Oracle) — prefixe AB_
# ===========================================================================

def generate_ab_assurance():
    print("Generation AbAssurance...")

    statuts_client = ["ACTIF", "INACTIF", "SUSPENDU"]
    types_assurance = ["SANTE", "HABITATION", "AUTO", "PROFESSIONNELLE"]
    statuts_contrat = ["ACTIF", "RESILIE", "SUSPENDU"]
    statuts_sinistre = ["DECLARE", "EN_COURS", "CLOTURE", "REJETE"]
    modes_paiement = ["PRELEVEMENT", "CARTE", "VIREMENT", "CHEQUE"]

    clients, contrats, sinistres, paiements = [], [], [], []

    for client_id in range(1, N_AB_CLIENTS + 1):
        prenom = fake.first_name()
        nom = fake.last_name()
        email = maybe_dirty_email(
            f"{prenom.lower()}.{nom.lower()}@{fake.free_email_domain()}"
        )
        clients.append({
            "AB_CLIENT_ID": client_id,
            "AB_NOM": nom,
            "AB_PRENOM": prenom,
            "AB_DATE_NAISSANCE": fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
            "AB_EMAIL": email,
            "AB_TELEPHONE": maybe_missing(fake.phone_number()),
            "AB_ADRESSE": fake.street_address(),
            "AB_CODE_POSTAL": fake.postcode(),
            "AB_NUM_FISCAL": fake.bothify(text="FR#########"),
            "AB_DATE_CREATION": fake.date_time_between(start_date="-5y", end_date="now").isoformat(),
            "AB_STATUT_CLIENT": random.choice(statuts_client),
        })

        # Quelques clients dupliques volontairement (defaut de qualite classique
        # apres une fusion d'entreprises : doublons de saisie).
        if random.random() < 0.02:
            dup = clients[-1].copy()
            dup["AB_CLIENT_ID"] = N_AB_CLIENTS + len(clients)  # id different, memes infos
            clients.append(dup)

    policy_counter = 1
    claim_counter = 1
    payment_counter = 1

    for client in clients:
        nb_contrats = random.randint(0, CONTRATS_PAR_CLIENT_MAX)
        for _ in range(nb_contrats):
            policy_number = f"AB-{policy_counter:06d}"
            policy_counter += 1
            date_debut = fake.date_between(start_date="-4y", end_date="-1M")
            date_fin = date_debut + timedelta(days=365)
            prime = round(random.uniform(200, 2500), 2)

            contrats.append({
                "AB_POLICY_NUMBER": policy_number,
                "AB_CLIENT_ID": client["AB_CLIENT_ID"],
                "AB_TYPE_ASSURANCE": random.choice(types_assurance),
                "AB_DATE_DEBUT": date_debut.isoformat(),
                "AB_DATE_FIN": date_fin.isoformat(),
                "AB_PRIME_ANNUELLE": prime,
                "AB_STATUT_CONTRAT": random.choice(statuts_contrat),
                "AB_AGENCE_ID": random.randint(1, 25),
            })

            # Sinistres lies a ce contrat
            if random.random() < SINISTRES_TAUX:
                for _ in range(random.randint(1, 2)):
                    sinistres.append({
                        "AB_CLAIM_ID": claim_counter,
                        "AB_POLICY_NUMBER": policy_number,
                        "AB_DATE_SINISTRE": fake.date_time_between(
                            start_date=date_debut, end_date="now"
                        ).isoformat(),
                        "AB_MONTANT_ESTIME": round(random.uniform(100, 15000), 2),
                        "AB_STATUT_SINISTRE": random.choice(statuts_sinistre),
                        "AB_DESCRIPTION": fake.sentence(nb_words=10),
                    })
                    claim_counter += 1

            # Paiements lies a ce contrat (mensualites ou paiement annuel)
            for _ in range(random.randint(1, PAIEMENTS_PAR_CONTRAT_MAX)):
                paiements.append({
                    "AB_PAYMENT_ID": payment_counter,
                    "AB_POLICY_NUMBER": policy_number,
                    "AB_DATE_PAIEMENT": fake.date_between(
                        start_date=date_debut, end_date="today"
                    ).isoformat(),
                    "AB_MONTANT": round(prime / 12, 2),
                    "AB_MODE_PAIEMENT": random.choice(modes_paiement),
                })
                payment_counter += 1

    base = os.path.join(OUTPUT_DIR, "ab_assurance")
    write_csv(clients, os.path.join(base, "ab_client.csv"), list(clients[0].keys()))
    write_csv(contrats, os.path.join(base, "ab_contrat.csv"), list(contrats[0].keys()))
    write_csv(sinistres, os.path.join(base, "ab_sinistre.csv"), list(sinistres[0].keys()))
    write_csv(paiements, os.path.join(base, "ab_paiement.csv"), list(paiements[0].keys()))


# ===========================================================================
# 2. BASE AssurePlus (SQL Server) — prefixe AP_
# ===========================================================================

def generate_assure_plus():
    print("Generation AssurePlus...")

    statuts_client = ["ACTIVE", "INACTIVE", "SUSPENDED"]
    product_codes = ["AUTO_BASIC", "AUTO_PREMIUM", "ASSIST_PREMIUM", "ASSIST_BASIC"]
    contract_states = ["ACTIVE", "TERMINATED", "SUSPENDED"]
    claim_statuses = ["REPORTED", "IN_PROGRESS", "CLOSED", "REJECTED"]
    payment_channels = ["CARD", "BANK_TRANSFER", "DIRECT_DEBIT"]
    transaction_statuses = ["SUCCESS", "PENDING", "FAILED"]

    users, contracts, claims, payments = [], [], [], []

    for user_id in range(1, N_AP_USERS + 1):
        full_name = fake.name()

        # AP_BIRTH_DATE est stocke en VARCHAR(10) cote source -> on simule
        # volontairement un format incoherent sur une partie des lignes
        # (defaut de qualite frequent sur des champs texte libres).
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=90)
        if random.random() < 0.05:
            birth_date_str = birth_date.strftime("%d/%m/%Y")  # format FR au lieu de YYYY-MM-DD
        else:
            birth_date_str = birth_date.isoformat()

        users.append({
            "AP_USER_ID": user_id,
            "AP_FULL_NAME": full_name,
            "AP_BIRTH_DATE": birth_date_str,
            "AP_MAIL_ADDRESS": maybe_dirty_email(
                full_name.lower().replace(" ", ".") + "@" + fake.free_email_domain()
            ),
            "AP_PHONE_NUMBER": maybe_missing(fake.phone_number()),
            "AP_STREET_ADDRESS": fake.street_address(),
            "AP_ZIP_CODE": fake.postcode(),
            "AP_CREATED_AT": fake.date_time_between(start_date="-5y", end_date="now").isoformat(sep=" "),
            "AP_CUSTOMER_STATUS": random.choice(statuts_client),
            "AP_LOYALTY_SCORE": random.randint(0, 100),
        })

    contract_counter = 1
    claim_counter = 1

    for user in users:
        nb_contracts = random.randint(0, CONTRATS_PAR_CLIENT_MAX)
        for _ in range(nb_contracts):
            contract_ref = f"AP-{contract_counter:06d}"
            contract_counter += 1
            start_date = fake.date_between(start_date="-4y", end_date="-1M")
            end_date = start_date + timedelta(days=365)
            monthly_premium = round(random.uniform(20, 250), 2)

            contracts.append({
                "AP_CONTRACT_REF": contract_ref,
                "AP_USER_ID": user["AP_USER_ID"],
                "AP_PRODUCT_CODE": random.choice(product_codes),
                "AP_START_DATE": start_date.isoformat(sep=" ") if False else start_date.isoformat(),
                "AP_END_DATE": end_date.isoformat(),
                "AP_MONTHLY_PREMIUM": monthly_premium,
                "AP_CONTRACT_STATE": random.choice(contract_states),
                "AP_BROKER_CODE": fake.bothify(text="BRK-###"),
            })

            if random.random() < SINISTRES_TAUX:
                for _ in range(random.randint(1, 2)):
                    claims.append({
                        "AP_SINISTRE_NUM": claim_counter,
                        "AP_CONTRACT_REF": contract_ref,
                        # format contraint a 19 caracteres cote source (VARCHAR(19))
                        "AP_INCIDENT_DATE": fake.date_time_between(
                            start_date=start_date, end_date="now"
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                        "AP_ESTIMATED_AMOUNT": round(random.uniform(100, 12000), 2),
                        "AP_CLAIM_STATUS": random.choice(claim_statuses),
                        "AP_CLAIM_COMMENT": fake.sentence(nb_words=10),
                        "AP_FRAUD_SCORE": round(random.uniform(0, 100), 2),
                    })
                    claim_counter += 1

            for _ in range(random.randint(1, PAIEMENTS_PAR_CONTRAT_MAX)):
                payments.append({
                    "AP_PAYMENT_REF": str(uuid.uuid4()),
                    "AP_CONTRACT_REF": contract_ref,
                    "AP_PAYMENT_DATETIME": fake.date_time_between(
                        start_date=start_date, end_date="now"
                    ).isoformat(sep=" "),
                    "AP_AMOUNT_PAID": monthly_premium,
                    "AP_PAYMENT_CHANNEL": random.choice(payment_channels),
                    "AP_TRANSACTION_STATUS": random.choice(transaction_statuses),
                })

    base = os.path.join(OUTPUT_DIR, "assure_plus")
    write_csv(users, os.path.join(base, "ap_users.csv"), list(users[0].keys()))
    write_csv(contracts, os.path.join(base, "ap_contracts.csv"), list(contracts[0].keys()))
    write_csv(claims, os.path.join(base, "ap_claims.csv"), list(claims[0].keys()))
    write_csv(payments, os.path.join(base, "ap_payments.csv"), list(payments[0].keys()))


if __name__ == "__main__":
    generate_ab_assurance()
    generate_assure_plus()
    print("\nTermine. Jeux de donnees disponibles dans le dossier 'data/'.")