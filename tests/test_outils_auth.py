# test_outils_auth.py — tests unitaires de app/outils_auth.py
import json

import pytest

from outils_auth import (
    authentifier,
    charger_utilisateurs,
    hacher_mot_de_passe,
    pages_autorisees,
    verifier_mot_de_passe,
)


def _comptes():
    return {
        "alice": {"hash": hacher_mot_de_passe("secret-admin"), "role": "admin"},
        "bob": {"hash": hacher_mot_de_passe("secret-user"), "role": "user"},
    }


def test_hash_different_a_chaque_fois_mais_verifiable():
    h1 = hacher_mot_de_passe("abc")
    h2 = hacher_mot_de_passe("abc")
    assert h1 != h2                      # sel aléatoire
    assert verifier_mot_de_passe("abc", h1)
    assert verifier_mot_de_passe("abc", h2)


def test_mauvais_mot_de_passe_refuse():
    assert not verifier_mot_de_passe("faux", hacher_mot_de_passe("abc"))


@pytest.mark.parametrize("hash_invalide", ["", "n'importe quoi", "md5$1$a$b", "pbkdf2_sha256$x$a$b", None])
def test_hash_invalide_ne_plante_pas(hash_invalide):
    assert verifier_mot_de_passe("abc", hash_invalide) is False


def test_authentifier_retourne_le_role():
    u = _comptes()
    assert authentifier(u, "alice", "secret-admin") == "admin"
    assert authentifier(u, "bob", "secret-user") == "user"


def test_authentifier_refuse_mauvais_mdp_ou_login_inconnu():
    u = _comptes()
    assert authentifier(u, "alice", "secret-user") is None
    assert authentifier(u, "inconnu", "secret-admin") is None
    assert authentifier(u, "", "") is None


def test_charger_utilisateurs_ok():
    assert "alice" in charger_utilisateurs(json.dumps(_comptes()))


@pytest.mark.parametrize("texte", [
    "pas du json", "[]", "{}", None,
    json.dumps({"x": {"role": "admin"}}),                    # hash manquant
    json.dumps({"x": {"hash": "h", "role": "superadmin"}}),  # rôle inconnu
])
def test_charger_utilisateurs_invalide(texte):
    with pytest.raises(ValueError):
        charger_utilisateurs(texte)


def test_pages_par_role():
    assert pages_autorisees("user") == ["dashboard", "fraude"]
    assert pages_autorisees("admin") == ["dashboard", "fraude", "services"]
    assert pages_autorisees("inconnu") == []