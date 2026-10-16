# generer_hash.py — fabrique la valeur de AUTH_USERS (comptes hachés).
#
# Usage, depuis la racine du projet :
#   python tools/generer_hash.py
# Puis copier le JSON affiché dans la variable/secret AUTH_USERS.

import getpass
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))
from outils_auth import ROLES, hacher_mot_de_passe  # noqa: E402

comptes = {}
while True:
    login = input("Identifiant (vide pour terminer) : ").strip()
    if not login:
        break
    role = input("Rôle (" + "/".join(ROLES) + ") : ").strip()
    if role not in ROLES:
        print("Rôle invalide.")
        continue
    mdp = getpass.getpass("Mot de passe : ")
    comptes[login] = {"hash": hacher_mot_de_passe(mdp), "role": role}

print("\nValeur de AUTH_USERS :\n")
print(json.dumps(comptes))
