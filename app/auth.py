# auth.py — écran de connexion Streamlit + contrôle des rôles (admin / user).
#
# Les comptes viennent de la variable d'environnement AUTH_USERS (sur Hugging Face :
# Settings > Variables and secrets > New secret), ou à défaut de .streamlit/secrets.toml.
# Voir tools/generer_hash.py pour fabriquer les hashs.
#
# Si aucun compte n'est configuré, l'application reste FERMÉE (jamais ouverte par défaut).

import os
import time

import streamlit as st

from outils_auth import authentifier, charger_utilisateurs

MAX_ECHECS = 5
BLOCAGE_SECONDES = 30


def _utilisateurs():
    texte = os.environ.get("AUTH_USERS")
    if not texte:
        try:
            texte = st.secrets.get("AUTH_USERS")
        except Exception:      # pas de fichier secrets.toml : normal en local
            texte = None
    if not texte:
        st.error("Aucun compte configuré : définir la variable AUTH_USERS (voir tools/generer_hash.py).")
        st.stop()
    try:
        return charger_utilisateurs(texte)
    except ValueError as erreur:
        st.error("Configuration des comptes invalide : " + str(erreur))
        st.stop()


def exiger_connexion():
    """Affiche le formulaire de connexion tant que l'utilisateur n'est pas identifié."""
    if st.session_state.get("role"):
        return

    st.title("Connexion")
    st.caption("Tableau de bord AbAssurance / AssurePlus")

    # Ralentit les essais en rafale : 5 échecs -> 30 s de pause
    bloque_jusqua = st.session_state.get("bloque_jusqua", 0)
    if time.time() < bloque_jusqua:
        st.error("Trop de tentatives. Réessaie dans " + str(int(bloque_jusqua - time.time()) + 1) + " s.")
        st.stop()

    utilisateurs = _utilisateurs()
    with st.form("connexion"):
        login = st.text_input("Identifiant")
        mot_de_passe = st.text_input("Mot de passe", type="password")
        envoye = st.form_submit_button("Se connecter")

    if envoye:
        role = authentifier(utilisateurs, login.strip(), mot_de_passe)
        if role:
            st.session_state["role"] = role
            st.session_state["login"] = login.strip()
            st.session_state["echecs"] = 0
            st.rerun()
        st.session_state["echecs"] = st.session_state.get("echecs", 0) + 1
        if st.session_state["echecs"] >= MAX_ECHECS:
            st.session_state["bloque_jusqua"] = time.time() + BLOCAGE_SECONDES
            st.session_state["echecs"] = 0
        st.error("Identifiant ou mot de passe incorrect.")
    st.stop()


def exiger_role(*roles):
    """À appeler en tête de chaque page protégée : bloque si le rôle n'est pas autorisé."""
    if st.session_state.get("role") not in roles:
        st.error("Accès réservé : rôle requis = " + " ou ".join(roles) + ".")
        st.stop()


def bouton_deconnexion():
    st.sidebar.caption("Connecté : " + st.session_state["login"] + " (" + st.session_state["role"] + ")")
    if st.sidebar.button("Se déconnecter"):
        for cle in ("role", "login"):
            st.session_state.pop(cle, None)
        st.rerun()
