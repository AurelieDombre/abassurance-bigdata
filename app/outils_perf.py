# outils_perf.py — le "carnet de chronos" du pipeline.
# Noter combien de temps a pris une étape.

import os
from datetime import datetime

FICHIER_PERF = "logs/performances.csv"


def noter_mesure(etape, duree):
    """Ajoute une ligne dans le carnet : la date, l'étape, et sa durée en secondes."""
    os.makedirs("logs", exist_ok=True)

    # Si le carnet n'existe pas encore, on écrit d'abord la ligne des titres
    nouveau = not os.path.exists(FICHIER_PERF)

    # "a" = on AJOUTE à la fin, on n'efface jamais les anciennes mesures
    with open(FICHIER_PERF, "a", encoding="utf-8") as fichier:
        if nouveau:
            fichier.write("date,etape,duree_s\n")
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fichier.write(date + "," + etape + "," + str(round(duree, 3)) + "\n")