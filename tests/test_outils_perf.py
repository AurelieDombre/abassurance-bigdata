# test_outils_perf.py
# Tests de la fonction noter_mesure(etape, duree).
#
# Lancer avec :  pytest -v test_outils_perf.py
#
# Idée générale : un test = on prépare, on appelle la fonction, on vérifie avec `assert`.
# Si un assert est faux, le test échoue.

import outils_perf


# `tmp_path` et `monkeypatch` sont fournis automatiquement par pytest :
# - tmp_path : un dossier temporaire vide, créé pour ce test seulement
# - monkeypatch.chdir(...) : on "se place" le dossier temporaire,
# et le vrai logs/performances.csv n'est jamais touché.


def test_cree_le_fichier_avec_les_titres(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    outils_perf.noter_mesure("lecture", 1.5)

    # On relit le fichier ligne par ligne
    with open("logs/performances.csv", encoding="utf-8") as f:
        lignes = f.read().splitlines()

    # Ligne 0 = les titres, ligne 1 = la mesure
    assert lignes[0] == "date,etape,duree_s"
    assert len(lignes) == 2


def test_la_ligne_contient_etape_et_duree(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    outils_perf.noter_mesure("lecture", 1.5)

    with open("logs/performances.csv", encoding="utf-8") as f:
        lignes = f.read().splitlines()

    # La ligne est "date,etape,duree" : on la coupe sur les virgules
    colonnes = lignes[1].split(",")
    assert colonnes[1] == "lecture"
    assert colonnes[2] == "1.5"


def test_les_titres_ne_sont_ecrits_qu_une_fois(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # On note deux mesures de suite
    outils_perf.noter_mesure("etape_1", 1)
    outils_perf.noter_mesure("etape_2", 2)

    with open("logs/performances.csv", encoding="utf-8") as f:
        lignes = f.read().splitlines()

    # 1 ligne de titres + 2 mesures = 3 lignes
    assert len(lignes) == 3
    assert lignes.count("date,etape,duree_s") == 1


def test_les_anciennes_mesures_sont_conservees(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    outils_perf.noter_mesure("premiere", 1)
    outils_perf.noter_mesure("seconde", 2)

    with open("logs/performances.csv", encoding="utf-8") as f:
        lignes = f.read().splitlines()

    # Les deux étapes sont toujours là, dans l'ordre : rien n'a été écrasé
    assert "premiere" in lignes[1]
    assert "seconde" in lignes[2]


def test_la_duree_est_arrondie_a_3_decimales(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    outils_perf.noter_mesure("calcul", 1.23456)

    with open("logs/performances.csv", encoding="utf-8") as f:
        lignes = f.read().splitlines()

    # 1.23456 arrondi à 3 décimales donne 1.235
    assert lignes[1].split(",")[2] == "1.235"


def test_dossier_non_inscriptible_ne_plante_pas(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # "logs" est un FICHIER : impossible de créer le dossier -> OSError, qu'on doit absorber
    (tmp_path / "logs").write_text("je bloque")

    assert outils_perf.noter_mesure("lecture", 1.5) is False
