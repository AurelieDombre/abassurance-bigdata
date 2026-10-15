# outils_auth.py — logique d'authentification SANS Streamlit (donc testable).
#
# Les mots de passe ne sont jamais stockés en clair : on garde un "hash"
# PBKDF2-SHA256 salé, au format  pbkdf2_sha256$itérations$sel$hash.
# On utilise uniquement la bibliothèque standard Python (hashlib, hmac).

import hashlib
import hmac
import json
import secrets

ROLES = ("admin", "user")
ITERATIONS = 200_000

# Pages visibles selon le rôle (clés utilisées par app.py)
PAGES_PAR_ROLE = {
    "admin": ["dashboard", "fraude", "services"],
    "user": ["dashboard"],
}

# Le sel= une chaîne aléatoire ajoutée au mot de passe avant le hachage.
def hacher_mot_de_passe(mot_de_passe, sel=None, iterations=ITERATIONS):
    """Retourne le hash d'un mot de passe, au format 'pbkdf2_sha256$iter$sel$hash'."""
    if sel is None:
        sel = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", mot_de_passe.encode("utf-8"), sel.encode("utf-8"), iterations)
    return "pbkdf2_sha256$" + str(iterations) + "$" + sel + "$" + h.hex()


def verifier_mot_de_passe(mot_de_passe, hache):
    """True si le mot de passe correspond au hash. Jamais d'exception : hash invalide -> False."""
    try:
        algo, iterations, sel, attendu = hache.split("$")
        if algo != "pbkdf2_sha256":
            return False
        calcule = hacher_mot_de_passe(mot_de_passe, sel, int(iterations))
    except (ValueError, AttributeError):
        return False
    # compare_digest : comparaison en temps constant (évite les attaques par chronométrage)
    return hmac.compare_digest(calcule.split("$")[3], attendu)


def charger_utilisateurs(texte_json):
    """Lit le JSON des comptes : {"login": {"hash": "...", "role": "admin|user"}, ...}.
    Lève ValueError si le format est invalide (on préfère planter que laisser un trou)."""
    try:
        donnees = json.loads(texte_json)
    except (TypeError, json.JSONDecodeError) as erreur:
        raise ValueError("AUTH_USERS n'est pas un JSON valide") from erreur
    if not isinstance(donnees, dict) or not donnees:
        raise ValueError("AUTH_USERS doit être un objet JSON non vide")
    for login, compte in donnees.items():
        if not isinstance(compte, dict) or "hash" not in compte:
            raise ValueError("Compte '" + str(login) + "' : champ 'hash' manquant")
        if compte.get("role") not in ROLES:
            raise ValueError("Compte '" + str(login) + "' : rôle invalide (admin ou user)")
    return donnees


# Hash factice : on le vérifie aussi quand le login n'existe pas,
# pour que la durée de réponse ne révèle pas si un compte existe.
_FAUX_HASH = hacher_mot_de_passe("faux-mot-de-passe", sel="0" * 32)


def authentifier(utilisateurs, login, mot_de_passe):
    """Retourne le rôle ('admin' ou 'user') si login/mot de passe sont bons, sinon None."""
    compte = utilisateurs.get(login)
    if compte is None:
        verifier_mot_de_passe(mot_de_passe, _FAUX_HASH)
        return None
    if verifier_mot_de_passe(mot_de_passe, compte["hash"]):
        return compte["role"]
    return None


def pages_autorisees(role):
    """Liste des pages accessibles pour un rôle (liste vide si rôle inconnu)."""
    return list(PAGES_PAR_ROLE.get(role, []))
