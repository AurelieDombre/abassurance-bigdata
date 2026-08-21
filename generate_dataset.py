"""
Ce script cree deux petits jeux de donnees de test :
- un pour AbAssurance (clients, contrats, sinistres, paiements)
- un pour AssurePlus (users, contracts, claims, payments)

Ces donnees sont fausses (generees automatiquement), mais elles
ressemblent a de vraies donnees d'assurance, pour pouvoir tester
le pipeline (Kafka, Spark, Hadoop) sans avoir de vraie base de donnees.

Pour lancer ce script :
    python generate_dataset.py
"""

import random
import uuid
from datetime import date, datetime, timedelta

import pandas as pd
from faker import Faker

# On fixe une "graine" aleatoire pour obtenir TOUJOURS les memes donnees generees afin que le pipeline soit reproductible.
random.seed(42)
Faker.seed(42)

# fake nous permet de generer des fausses donnees realistes (noms, adresses, etc.)
fake = Faker("fr_FR")

# Combien de clients/users on veut generer
NOMBRE_CLIENTS_AB = 200
NOMBRE_USERS_AP = 100


def date_aleatoire_recente(annees_max=4):
    """Retourne une date aleatoire entre aujourd'hui et 'annees_max' annees dans le passe."""
    jours_max = annees_max * 365
    jours_avant = random.randint(30, jours_max)
    return date.today() - timedelta(days=jours_avant)


def email_avec_defauts(email):
    """
    La plupart du temps, on renvoie l'email tel quel.
    Mais parfois (pour simuler des vraies donnees imparfaites) :
    - on renvoie un email vide
    - ou un email mal ecrit
    """
    tirage = random.random()  # nombre aleatoire entre 0 et 1

    if tirage < 0.02:
        return ""  # email manquant
    elif tirage < 0.04:
        return email.replace("@", "_at_")  # email mal ecrit
    else:
        return email  # email normal


def telephone_avec_defauts(telephone):
    """Simule un numero de telephone parfois manquant."""
    if random.random() < 0.04:
        return ""
    return telephone


# ===========================================================================
# ETAPE 1 : Generer les donnees AbAssurance
# ===========================================================================

def generer_clients_ab():
    """Genere la liste des clients AbAssurance."""
    print("Generation des clients AbAssurance...")
    liste_clients = []

    for i in range(1, NOMBRE_CLIENTS_AB + 1):
        prenom = fake.first_name()
        nom = fake.last_name()
        email = prenom.lower() + "." + nom.lower() + "@" + fake.free_email_domain()

        client = {
            "AB_CLIENT_ID": i,
            "AB_NOM": nom,
            "AB_PRENOM": prenom,
            "AB_DATE_NAISSANCE": fake.date_of_birth(minimum_age=18, maximum_age=90),
            "AB_EMAIL": email_avec_defauts(email),
            "AB_TELEPHONE": telephone_avec_defauts(fake.phone_number()),
            "AB_ADRESSE": fake.street_address(),
            "AB_CODE_POSTAL": fake.postcode(),
            "AB_NUM_FISCAL": fake.bothify(text="FR#########"),
            "AB_DATE_CREATION": fake.date_time_between(start_date="-5y", end_date="now"),
            "AB_STATUT_CLIENT": random.choice(["ACTIF", "INACTIF", "SUSPENDU"]),
        }
        liste_clients.append(client)

    return liste_clients


def generer_contrats_sinistres_paiements_ab(liste_clients):
    """
    Pour chaque client AbAssurance, on genere entre 0 et 3 contrats.
    Pour chaque contrat, on genere parfois des sinistres et des paiements.
    """
    print("Generation des contrats/sinistres/paiements AbAssurance...")

    liste_contrats = []
    liste_sinistres = []
    liste_paiements = []

    numero_contrat = 1
    numero_sinistre = 1
    numero_paiement = 1

    for client in liste_clients:
        nombre_contrats = random.randint(0, 3)

        for _ in range(nombre_contrats):
            reference_contrat = "AB-" + str(numero_contrat).zfill(6)
            numero_contrat = numero_contrat + 1

            date_debut = date_aleatoire_recente()
            date_fin = date_debut + timedelta(days=365)
            prime_annuelle = round(random.uniform(200, 2500), 2)

            contrat = {
                "AB_POLICY_NUMBER": reference_contrat,
                "AB_CLIENT_ID": client["AB_CLIENT_ID"],
                "AB_TYPE_ASSURANCE": random.choice(["SANTE", "HABITATION", "AUTO", "PROFESSIONNELLE"]),
                "AB_DATE_DEBUT": date_debut,
                "AB_DATE_FIN": date_fin,
                "AB_PRIME_ANNUELLE": prime_annuelle,
                "AB_STATUT_CONTRAT": random.choice(["ACTIF", "RESILIE", "SUSPENDU"]),
                "AB_AGENCE_ID": random.randint(1, 25),
            }
            liste_contrats.append(contrat)

            # 25% de chance d'avoir un sinistre sur ce contrat
            if random.random() < 0.25:
                sinistre = {
                    "AB_CLAIM_ID": numero_sinistre,
                    "AB_POLICY_NUMBER": reference_contrat,
                    "AB_DATE_SINISTRE": fake.date_time_between(start_date=date_debut, end_date="now"),
                    "AB_MONTANT_ESTIME": round(random.uniform(100, 15000), 2),
                    "AB_STATUT_SINISTRE": random.choice(["DECLARE", "EN_COURS", "CLOTURE", "REJETE"]),
                    "AB_DESCRIPTION": fake.sentence(nb_words=10),
                }
                liste_sinistres.append(sinistre)
                numero_sinistre = numero_sinistre + 1

            # Entre 1 et 6 paiements sur ce contrat (ex: mensualites)
            nombre_paiements = random.randint(1, 6)
            for _ in range(nombre_paiements):
                paiement = {
                    "AB_PAYMENT_ID": numero_paiement,
                    "AB_POLICY_NUMBER": reference_contrat,
                    "AB_DATE_PAIEMENT": fake.date_between(start_date=date_debut, end_date="today"),
                    "AB_MONTANT": round(prime_annuelle / 12, 2),
                    "AB_MODE_PAIEMENT": random.choice(["PRELEVEMENT", "CARTE", "VIREMENT", "CHEQUE"]),
                }
                liste_paiements.append(paiement)
                numero_paiement = numero_paiement + 1

    return liste_contrats, liste_sinistres, liste_paiements


# ===========================================================================
# ETAPE 2 : Generer les donnees AssurePlus
# ===========================================================================

def generer_users_ap():
    """Genere la liste des users AssurePlus."""
    print("Generation des users AssurePlus...")
    liste_users = []

    for i in range(1, NOMBRE_USERS_AP + 1):
        nom_complet = fake.name()
        email = nom_complet.lower().replace(" ", ".") + "@" + fake.free_email_domain()
        date_naissance = fake.date_of_birth(minimum_age=18, maximum_age=90)

        # AssurePlus stocke la date de naissance en texte libre (VARCHAR).
        # Dans la vraie vie, ca cree souvent des formats incoherents.
        # On simule ca ici : 5% des dates sont au format francais JJ/MM/AAAA
        # au lieu du format normal AAAA-MM-JJ.
        if random.random() < 0.05:
            date_naissance_texte = date_naissance.strftime("%d/%m/%Y")
        else:
            date_naissance_texte = date_naissance.isoformat()

        user = {
            "AP_USER_ID": i,
            "AP_FULL_NAME": nom_complet,
            "AP_BIRTH_DATE": date_naissance_texte,
            "AP_MAIL_ADDRESS": email_avec_defauts(email),
            "AP_PHONE_NUMBER": telephone_avec_defauts(fake.phone_number()),
            "AP_STREET_ADDRESS": fake.street_address(),
            "AP_ZIP_CODE": fake.postcode(),
            "AP_CREATED_AT": fake.date_time_between(start_date="-5y", end_date="now"),
            "AP_CUSTOMER_STATUS": random.choice(["ACTIVE", "INACTIVE", "SUSPENDED"]),
            "AP_LOYALTY_SCORE": random.randint(0, 100),
        }
        liste_users.append(user)

    return liste_users


def generer_contracts_claims_payments_ap(liste_users):
    """
    Pour chaque user AssurePlus, on genere entre 0 et 3 contrats.
    Pour chaque contrat, on genere parfois des claims et des payments.
    """
    print("Generation des contracts/claims/payments AssurePlus...")

    liste_contracts = []
    liste_claims = []
    liste_payments = []

    numero_contract = 1
    numero_claim = 1

    for user in liste_users:
        nombre_contracts = random.randint(0, 3)

        for _ in range(nombre_contracts):
            reference_contract = "AP-" + str(numero_contract).zfill(6)
            numero_contract = numero_contract + 1

            date_debut = date_aleatoire_recente()
            date_fin = date_debut + timedelta(days=365)
            prime_mensuelle = round(random.uniform(20, 250), 2)

            contract = {
                "AP_CONTRACT_REF": reference_contract,
                "AP_USER_ID": user["AP_USER_ID"],
                "AP_PRODUCT_CODE": random.choice(["AUTO_BASIC", "AUTO_PREMIUM", "ASSIST_PREMIUM", "ASSIST_BASIC"]),
                "AP_START_DATE": date_debut,
                "AP_END_DATE": date_fin,
                "AP_MONTHLY_PREMIUM": prime_mensuelle,
                "AP_CONTRACT_STATE": random.choice(["ACTIVE", "TERMINATED", "SUSPENDED"]),
                "AP_BROKER_CODE": fake.bothify(text="BRK-###"),
            }
            liste_contracts.append(contract)

            # 25% de chance d'avoir un claim sur ce contrat
            if random.random() < 0.25:
                date_incident = fake.date_time_between(start_date=date_debut, end_date="now")
                claim = {
                    "AP_SINISTRE_NUM": numero_claim,
                    "AP_CONTRACT_REF": reference_contract,
                    # format texte a 19 caracteres, ex: "2026-01-15 10:30:00"
                    "AP_INCIDENT_DATE": date_incident.strftime("%Y-%m-%d %H:%M:%S"),
                    "AP_ESTIMATED_AMOUNT": round(random.uniform(100, 12000), 2),
                    "AP_CLAIM_STATUS": random.choice(["REPORTED", "IN_PROGRESS", "CLOSED", "REJECTED"]),
                    "AP_CLAIM_COMMENT": fake.sentence(nb_words=10),
                    "AP_FRAUD_SCORE": round(random.uniform(0, 100), 2),
                }
                liste_claims.append(claim)
                numero_claim = numero_claim + 1

            # Entre 1 et 6 payments sur ce contrat
            nombre_payments = random.randint(1, 6)
            for _ in range(nombre_payments):
                payment = {
                    "AP_PAYMENT_REF": str(uuid.uuid4()),
                    "AP_CONTRACT_REF": reference_contract,
                    "AP_PAYMENT_DATETIME": fake.date_time_between(start_date=date_debut, end_date="now"),
                    "AP_AMOUNT_PAID": prime_mensuelle,
                    "AP_PAYMENT_CHANNEL": random.choice(["CARD", "BANK_TRANSFER", "DIRECT_DEBIT"]),
                    "AP_TRANSACTION_STATUS": random.choice(["SUCCESS", "PENDING", "FAILED"]),
                }
                liste_payments.append(payment)

    return liste_contracts, liste_claims, liste_payments


# ===========================================================================
# ETAPE 3 : Sauvegarder tout en fichiers CSV
# ===========================================================================

def sauvegarder_en_csv(liste_de_dictionnaires, chemin_fichier):
    """
    Transforme une liste de dictionnaires en tableau (DataFrame pandas),
    puis l'enregistre dans un fichier CSV.
    """
    tableau = pd.DataFrame(liste_de_dictionnaires)
    tableau.to_csv(chemin_fichier, index=False)
    print("  -> Fichier cree :", chemin_fichier, "(", len(tableau), "lignes )")


# ===========================================================================
# PROGRAMME PRINCIPAL
# ===========================================================================

if __name__ == "__main__":

    # --- AbAssurance ---
    clients_ab = generer_clients_ab()
    contrats_ab, sinistres_ab, paiements_ab = generer_contrats_sinistres_paiements_ab(clients_ab)

    sauvegarder_en_csv(clients_ab, "data/ab_assurance/ab_client.csv")
    sauvegarder_en_csv(contrats_ab, "data/ab_assurance/ab_contrat.csv")
    sauvegarder_en_csv(sinistres_ab, "data/ab_assurance/ab_sinistre.csv")
    sauvegarder_en_csv(paiements_ab, "data/ab_assurance/ab_paiement.csv")

    # --- AssurePlus ---
    users_ap = generer_users_ap()
    contracts_ap, claims_ap, payments_ap = generer_contracts_claims_payments_ap(users_ap)

    sauvegarder_en_csv(users_ap, "data/assure_plus/ap_users.csv")
    sauvegarder_en_csv(contracts_ap, "data/assure_plus/ap_contracts.csv")
    sauvegarder_en_csv(claims_ap, "data/assure_plus/ap_claims.csv")
    sauvegarder_en_csv(payments_ap, "data/assure_plus/ap_payments.csv")

    print("\nTermine ! Les fichiers sont dans le dossier data/")