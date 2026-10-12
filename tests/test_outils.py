
# test_outils.py — tests unitaires de app/outils.py
# https://docs.pytest.org/en/stable/how-to/assert.html
#
# Pour les lancer, depuis la racine du projet :
#   pytest
#   pytest --cov=app --cov-report=term-missing --cov-report=xml

import socket

import pandas as pd

from outils import (
    construire_mail,
    detecter_ralentissement,
    formater_ligne_log,
    generer_rapport_pdf,
    interpreter_datanodes,
    tester_porte,
    texte_pdf,
    trouver_pannes,
    vers_csv,
)


# ==================================================================
# vers_csv : tableau pandas -> texte CSV pour Excel
# ==================================================================
def faux_clients():
    """Une mini-table clients inventée, avec les colonnes réellement
    exportées par le tableau de bord (pas d'email ni de téléphone)."""
    return pd.DataFrame({
        "client_id": ["C001"],
        "code_postal": ["75001"],
        "statut_client": ["résilié"],
    })
    
def test_vers_csv_commence_marqueur_excel():
    # Le "marqueur invisible" (BOM) permet à Excel d'afficher les accents.
    resultat = vers_csv(faux_clients())
    assert resultat.startswith(b"\xef\xbb\xbf")


def test_vers_csv_utilise_point_virgule_accents():
    # On redécode le résultat pour le relire comme du texte normal
    texte = vers_csv(faux_clients()).decode("utf-8-sig")
    assert texte.splitlines() == [
        "client_id;code_postal;statut_client",
        "C001;75001;résilié",
    ]


def test_vers_csv_n_ajoute_pas_de_colonne_numero():
    # index=False : pas de colonne 0, 1, 2... ajoutée toute seule par pandas
    texte = vers_csv(faux_clients()).decode("utf-8-sig")
    assert texte.splitlines()[0] == "client_id;code_postal;statut_client"


# ==================================================================
# texte_pdf : remplace les caractères que le PDF ne sait pas écrire
# ==================================================================
def test_texte_pdf_garde_accents():
    # é, à, ç... existent dans la police de base : on ne doit pas les abîmer
    assert texte_pdf("Café à la crème") == "Café à la crème"

# remplace les caractères inconnus par un point d'interrogation
def test_texte_pdf_remplace_caracteres_inconnus():
    assert texte_pdf("Bonjour 😀") == "Bonjour ?"


def test_texte_pdf_texte_vide():
    assert texte_pdf("") == ""


# ==================================================================
# generer_rapport_pdf : fabrique le rapport
# ==================================================================
def faux_resultats():
    """Des résultats inventés, comme ceux que renvoie calculer() dans app.py."""
    return {
        "duree": 1.234,
        "nb_clients": 10,
        "nb_contrats": 20,
        "nb_paiements": 30,
        "nb_sinistres": 5,
        "total_sinistres": 12345.678,
        "contrats_par_statut": {"ACTIF": 15, "RESILIE": 5},
    }


def test_rapport_pdf_est_un_vrai_pdf():
    pdf = generer_rapport_pdf(faux_resultats())
    assert isinstance(pdf, bytes)
    # Un fichier PDF commence toujours par ces 5 caractères
    assert pdf.startswith(b"%PDF-")


def test_rapport_pdf_sans_aucun_statut_ne_plante_pas():
    resultats = faux_resultats()
    resultats["contrats_par_statut"] = {}   # aucun contrat trouvé
    pdf = generer_rapport_pdf(resultats)
    assert pdf.startswith(b"%PDF-")


# ==================================================================
# tester_porte : "frapper à la porte" d'un service
# ==================================================================
def test_serveur_repond():
    # On ouvre nous-mêmes une porte (un mini serveur sur notre machine),
    # puis on vérifie que tester_porte la trouve ouverte.
    serveur = socket.socket()
    serveur.bind(("127.0.0.1", 0))   # port 0 = "donne-moi n'importe quel port libre"
    serveur.listen()
    port = serveur.getsockname()[1]
    try:
        assert tester_porte("127.0.0.1", port) is True
    finally:
        serveur.close()   # on referme la porte, même si le test a échoué


def test_serveur_repond_pas():
    # On réserve un port libre puis on referme : il n'y a plus personne derrière.
    serveur = socket.socket()
    serveur.bind(("127.0.0.1", 0))
    port = serveur.getsockname()[1]
    serveur.close()
    assert tester_porte("127.0.0.1", port) is False


def test_serveur_inconnue_reponse():
    # Un nom qui n'existe pas doit donner False, pas une erreur.
    assert tester_porte("machine-qui-n-existe-pas.invalid", 80) is False


# ==================================================================
# interpreter_datanodes : le voyant Hadoop
# ==================================================================
def test_hadoop_2_datanodes_sur_2_vert():
    ok, message = interpreter_datanodes(vivants=2, silencieux=0)
    assert ok is True
    assert "2 datanodes sur 2" in message

#test_hadoop_1_datanode_sur_2_est_rouge_mais_les_donnees_restent_lisibles
def test_hadoop_datanode_1_sur_2_rouge():
    # Cas qu'on montre à l'oral : un datanode tombe, l'autre prend le relais.
    ok, message = interpreter_datanodes(vivants=1, silencieux=0)
    assert ok is False
    assert "1 datanode sur 2" in message
    assert "lisibles" in message


def test_hadoop_datanode_silencieux():
    # 2 vivants mais 1 silencieux (pas de signal depuis 30 s) = 1 seul en bonne santé
    ok, message = interpreter_datanodes(vivants=2, silencieux=1)
    assert ok is False
    assert "1 datanode sur 2" in message


def test_hadoop_aucun_datanode_rouge():
    ok, message = interpreter_datanodes(vivants=0, silencieux=0)
    assert ok is False
    assert "inaccessibles" in message


def test_hadoop_deux_silencieux_inaccessibles():
    ok, message = interpreter_datanodes(vivants=2, silencieux=2)
    assert ok is False
    assert "inaccessibles" in message


def test_hadoop_ajout_datanodes():
    # Si on ajoute un 3e datanode un jour, ça ne doit pas passer au rouge.
    ok, _ = interpreter_datanodes(vivants=3, silencieux=0)
    assert ok is True


def test_hadoop_nombre_attendu_personnalise():
    # Avec 3 datanodes attendus, en avoir 2 est un état dégradé.
    ok, message = interpreter_datanodes(vivants=2, silencieux=0, attendus=3)
    assert ok is False
    assert "2 datanode sur 3" in message


# ==================================================================
# trouver_pannes : quelles étapes sont en rouge ?
# ==================================================================
def test_trouver_pannes_succes():
    resultats = [("Kafka", True, "ok"), ("Hadoop", True, "ok")]
    assert trouver_pannes(resultats) == []


def test_trouver_pannes_une_panne():
    resultats = [("Kafka", True, "ok"), ("Hadoop", False, "en panne")]
    assert trouver_pannes(resultats) == ["Hadoop"]


def test_trouver_pannes_plusieurs_etapes_panne_ordre():
    resultats = [("Kafka", False, "x"), ("Hadoop", True, "ok"), ("Spark", False, "y")]
    assert trouver_pannes(resultats) == ["Kafka", "Spark"]


def test_trouver_pannes_liste_vide():
    assert trouver_pannes([]) == []


# ==================================================================
# formater_ligne_log : une ligne du carnet de bord
# ==================================================================
def test_ligne_log_etape_ok():
    ligne = formater_ligne_log("2026-10-06 14:32:10", "Kafka", True, "Kafka répond")
    assert ligne == "2026-10-06 14:32:10 | OK | Kafka | Kafka répond"


def test_ligne_log_etape_panne():
    ligne = formater_ligne_log("2026-10-06 14:32:10", "Hadoop", False, "namenode injoignable")
    assert ligne == "2026-10-06 14:32:10 | ALERTE | Hadoop | namenode injoignable"


# ==================================================================
# construire_mail : la lettre d'alerte (sans l'envoyer)
# ==================================================================
def test_mail_sujet_contient_services_panne():
    mail = construire_mail(["Kafka", "Hadoop"])
    assert mail["Subject"] == "ALERTE pipeline : Kafka, Hadoop"


def test_mail_texte_contient_services_panne():
    mail = construire_mail(["Spark"])
    assert "Spark" in mail.get_content()


def test_mail_adresses_par_defaut():
    mail = construire_mail(["Kafka"])
    assert mail["From"] == "pipeline@abassurance.local"
    assert mail["To"] == "responsable@abassurance.local"


def test_mail_adresses_personnalisees():
    mail = construire_mail(["Kafka"], expediteur="a@test.fr", destinataire="b@test.fr")
    assert mail["From"] == "a@test.fr"
    assert mail["To"] == "b@test.fr"


# ==================================================================
# detecter_ralentissement : la dernière durée est-elle anormale ?
# ==================================================================
def test_ralentissement_mesures():
    # Avec 3 mesures, on n'a pas encore d'"habitude" : on ne peut pas juger.
    assert detecter_ralentissement([1, 1, 1]) == (False, None, None)

# Si temps stable pas de ralentissement
def test_ralentissement_temps_stable():
    ralenti, derniere, moyenne = detecter_ralentissement([2, 2, 2, 2])
    assert ralenti is False
    assert derniere == 2
    assert moyenne == 2

# Si la dernière mesure dépasse 2 fois la moyenne des précédentes, c'est un ralentissement
def test_ralentissement_detecte_derniere_depasse_moyenne():
    # D'habitude 1 seconde, cette fois 5 secondes
    ralenti, derniere, moyenne = detecter_ralentissement([1, 1, 1, 5])
    assert ralenti is True
    assert derniere == 5
    assert moyenne == 1


def test_ralentissement_ignore_ecarts_sous_seconde():
    # 0,1 s -> 0,5 s : c'est 5 fois plus long, mais trop petit pour compter
    ralenti, _, _ = detecter_ralentissement([0.1, 0.1, 0.1, 0.5])
    assert ralenti is False

# Si la dernière mesure est exactement 2 fois la moyenne, ce n'est pas un ralentissement
def test_ralentissement_exactement_2_fois_moyenne():
    # La règle est "plus de 2 fois" : 4 s contre une moyenne de 2 s ne suffit pas.
    ralenti, _, _ = detecter_ralentissement([2, 2, 2, 4])
    assert ralenti is False


def test_ralentissement_facteur_personnalise():
    # Avec un facteur de 1,5, passer de 2 s à 4 s devient un ralentissement.
    ralenti, _, _ = detecter_ralentissement([2, 2, 2, 4], facteur=1.5)
    assert ralenti is True


def test_ralentissement_amelioration():
    # Si la dernière mesure est plus rapide, tout va bien.
    ralenti, _, _ = detecter_ralentissement([5, 5, 5, 1])
    assert ralenti is False
