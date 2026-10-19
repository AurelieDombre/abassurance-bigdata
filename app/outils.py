# outils.py — la "boîte à outils" du projet.
#
# Ici, on range les fonctions qui ne font QUE du calcul : elles reçoivent
# quelque chose, elles renvoient un résultat, et c'est tout. Elles n'affichent
# rien (pas de Streamlit) et n'ont pas besoin de Spark ni de Hadoop.


import socket
from datetime import datetime
from email.message import EmailMessage

from fpdf import FPDF


# ------------------------------------------------------------------
# 1. Export des données (venait de app.py)
# ------------------------------------------------------------------
def vers_csv(tableau_pandas):
    """Transforme un tableau pandas en texte CSV, prêt à être téléchargé.
    encoding="utf-8-sig" = ajoute un petit marqueur invisible au début du
    fichier pour qu'Excel affiche bien les accents (é, è, à...) sans bug."""
    return tableau_pandas.to_csv(index=False, sep=";").encode("utf-8-sig")


def probas_en_pourcentage(tableau_pandas, colonne="proba_fraude"):
    """Prépare la liste des sinistres pour l'export CSV : la probabilité de fraude,
    rangée par le modèle entre 0 et 1 (0.875), devient un pourcentage entier (88).

    Pourquoi un nombre entier ? Dans un Excel français, « 87.5 » (avec un point)
    risque d'être lu comme du texte. « 88 » se lit partout pareil.
    Image : on traduit la note sur 1 en note sur 100, plus parlante pour un humain.

    Le tableau d'origine n'est pas modifié : on travaille sur une copie
    (comme on annote une photocopie, pas l'original)."""
    copie = tableau_pandas.copy()
    # * 100 puis arrondi à l'unité. "Int64" (avec un I majuscule) = entier qui
    # accepte les cases vides : une probabilité manquante reste vide au lieu de planter.
    copie[colonne] = (copie[colonne] * 100).round().astype("Int64")
    # On renomme la colonne pour que l'unité soit écrite dans le fichier
    return copie.rename(columns={colonne: "proba_fraude (%)"})


def texte_pdf(texte):
    """fpdf2 (police de base) ne connaît pas tous les caractères Unicode.
    On remplace ceux qu'il ne sait pas afficher, pour ne jamais planter."""
    return texte.encode("latin-1", "replace").decode("latin-1")


def generer_rapport_pdf(resultats):
    """Construit un petit rapport PDF avec les chiffres clés.
    On procède comme pour écrire une page à la main : on pose du texte
    ligne par ligne, du haut vers le bas."""
    pdf = FPDF()
    pdf.add_page()

    # --- Titre
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, texte_pdf("Rapport AbAssurance / AssurePlus"), new_x="LMARGIN", new_y="NEXT")

    # --- Date de génération
    pdf.set_font("Helvetica", "", 10)
    date_du_jour = datetime.now().strftime("%d/%m/%Y a %H:%M")
    pdf.cell(0, 8, texte_pdf("Genere le " + date_du_jour), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, texte_pdf("Temps de traitement Spark : " + str(round(resultats["duree"], 1)) + " secondes"), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)   # ln(6) = saut d'une petite ligne vide, pour aérer

    # --- Chiffres clés
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, texte_pdf("Chiffres cles"), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, texte_pdf("Clients : " + str(resultats["nb_clients"])), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, texte_pdf("Contrats : " + str(resultats["nb_contrats"])), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, texte_pdf("Paiements : " + str(resultats["nb_paiements"])), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, texte_pdf("Sinistres : " + str(resultats["nb_sinistres"])), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, texte_pdf("Montant total estime des sinistres : " + str(round(resultats["total_sinistres"], 2))), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # --- Répartition des contrats par statut
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, texte_pdf("Contrats par statut"), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 11)
    for statut, nombre in resultats["contrats_par_statut"].items():
        pdf.cell(0, 8, texte_pdf(statut + " : " + str(nombre)), new_x="LMARGIN", new_y="NEXT")

    # pdf.output() renvoie le PDF sous forme de bytes, directement
    # utilisables par le bouton de téléchargement (pas besoin de fichier).
    return bytes(pdf.output())


# ------------------------------------------------------------------
# 2. Surveillance des services (venait de Etat_des_services.py)
# ------------------------------------------------------------------
def tester_porte(nom_machine, port):
    """Frappe à la porte d'un service. Si quelqu'un ouvre : True, sinon : False."""
    try:
        porte = socket.create_connection((nom_machine, port), timeout=3)
        porte.close()
        return True
    except Exception:
        return False


def interpreter_datanodes(vivants, silencieux=0, attendus=2):
    """Décide si Hadoop est en bonne santé, à partir du nombre de datanodes.

    - vivants    : datanodes que le namenode n'a pas encore déclarés morts
    - silencieux : parmi eux, ceux qui n'ont rien dit depuis 30 secondes
    - attendus   : combien on en attend (2, car la réplication est réglée sur 2)

    Renvoie (True ou False, un message), comme les fonctions verifier_...

    Image : deux entrepôts qui gardent chacun une photocopie de tout.
    Si un seul répond, rien n'est perdu, mais il n'y a plus de sécurité."""
    # Les datanodes en bonne santé = vivants moins ceux qui sont silencieux
    en_bonne_sante = vivants - silencieux

    if en_bonne_sante >= attendus:
        return True, (str(attendus) + " datanodes sur " + str(attendus)
                      + " vivants : données copiées en double")
    if en_bonne_sante >= 1:
        # Il en reste au moins un : il a encore une copie de tout.
        return False, (str(en_bonne_sante) + " datanode sur " + str(attendus)
                       + " : données lisibles grâce à l'autre, mais plus de copie de secours")
    return False, "aucun datanode vivant : les données sont inaccessibles"


def trouver_pannes(resultats):
    """Parmi les résultats [(nom, ok, message), ...], garde seulement
    les noms des étapes dont le voyant est rouge (ok vaut False)."""
    en_panne = []
    for nom, ok, message in resultats:
        if not ok:
            en_panne.append(nom)
    return en_panne


def formater_ligne_log(date, nom, ok, message):
    """Fabrique UNE ligne du carnet de bord (le fichier de logs).
    Exemple : 2026-10-06 14:32:10 | ALERTE | Hadoop | 1 datanode sur 2..."""
    if ok:
        niveau = "OK"
    else:
        niveau = "ALERTE"
    return date + " | " + niveau + " | " + nom + " | " + message


def construire_mail(pannes,
                    expediteur="pipeline@abassurance.local",
                    destinataire="responsable@abassurance.local"):
    """Prépare la lettre d'alerte (sujet, expéditeur, destinataire, texte).
    Elle n'est PAS envoyée ici : la page Streamlit la dépose ensuite
    chez le facteur (Mailpit). Séparer les deux permet de tester
    le contenu de la lettre sans avoir besoin d'un facteur."""
    liste = ", ".join(pannes)
    mail = EmailMessage()
    mail["Subject"] = "ALERTE pipeline : " + liste
    mail["From"] = expediteur
    mail["To"] = destinataire
    mail.set_content("Problème détecté sur : " + liste)
    return mail


# ------------------------------------------------------------------
# 3. Détection de ralentissement (venait de la section Performances)
# ------------------------------------------------------------------
def detecter_ralentissement(durees, facteur=2, minimum_s=1, nb_mesures_min=4):
    """Regarde si la DERNIÈRE durée est anormalement longue.

    - durees         : liste des temps mesurés (en secondes), du plus ancien au plus récent
    - facteur        : on s'inquiète si la dernière dépasse facteur x la moyenne des précédentes
    - minimum_s      : on ignore les écarts sous cette valeur (trop petits pour compter)
    - nb_mesures_min : il faut au moins ce nombre de mesures pour avoir une "habitude"

    Renvoie (ralentissement ou non, dernière durée, moyenne des précédentes).
    Si on n'a pas assez de mesures, renvoie (False, None, None).

    Image : un coureur qui fait d'habitude 10 minutes et en met soudain 25."""
    if len(durees) < nb_mesures_min:
        return False, None, None

    derniere = durees[-1]
    precedentes = durees[:-1]
    moyenne = sum(precedentes) / len(precedentes)

    ralenti = derniere > facteur * moyenne and derniere > minimum_s
    return ralenti, derniere, moyenne