# Version en ligne et version complète : ce qui fonctionne où

> Retour : [README](../README.md) · [Journal technique](./journal_technique.md)

Le projet AbAssurance / AssurePlus existe en **deux versions** de l'application Streamlit :

| | Version complète (locale) | Version en ligne (déployée) |
| --- | --- | --- |
| Dossier | `app/` | `en_ligne/` |
| Où elle tourne | Sur le PC, dans Docker | Streamlit Community Cloud |
| Source des données | Hadoop (HDFS), alimenté en continu par Kafka | Fichiers Parquet exportés de HDFS (dossier `en_ligne/donnees`) |
| Traitements | Spark (PySpark) | pandas |
| Usage | Démonstration en direct (vidéo de soutenance) | Consultation et correction par le jury |

En résumé, la version en ligne est une **vitrine** : elle montre les résultats du pipeline. La version complète est **l'atelier** : elle contient le pipeline qui produit ces résultats.

---

## 1. Pourquoi deux versions ?

Le pipeline complet repose sur plusieurs services lourds qui tournent dans des conteneurs Docker :

- **Kafka** reçoit les données publiées par Talaxie.
- **Hadoop** (un namenode et deux datanodes) stocke ces données.
- **Spark** les traite et entraîne le modèle de fraude.
- **Mailpit** reçoit les mails d'alerte.

Un hébergement gratuit comme Streamlit Community Cloud ne peut pas faire tourner cet ensemble, pour trois raisons : la mémoire est limitée, Java n'est pas disponible et il n'est pas possible d'y lancer plusieurs conteneurs. Héberger toute la stack en ligne demanderait un serveur payant et dédié.

Le choix retenu correspond à l'option de repli acceptée par le formateur. La version en ligne lit des **exports** des tables produites par le pipeline. Le jury peut ainsi se connecter et consulter les résultats sans rien installer.

Par conséquent, **les données en ligne sont un instantané** : elles reflètent l'état de Hadoop au moment de l'export, et ne se mettent pas à jour toutes seules.

---

## 2. Tableau des fonctionnalités

| Fonctionnalité | Complète | En ligne | Pourquoi |
| --- | :---: | :---: | --- |
| Connexion avec rôles admin / user | ✅ | ✅ | Même module `auth` et mêmes comptes (`AUTH_USERS`). |
| Tableau de bord : chiffres clés, contrats par statut, sinistres | ✅ | ✅ | Calculs simples, faits avec pandas sur les exports. |
| Export CSV (sans colonnes sensibles) | ✅ | ✅ | Identique dans les deux versions. |
| Import de ses propres fichiers (CSV ou Parquet) | ❌ | ✅ | Pensé pour le correcteur, voir la section 5. Dans la version complète, les données arrivent par le pipeline. |
| Fraude : qualité du modèle (précision, rappel) | ✅ | ✅ | Métriques déjà calculées par Spark, exportées en Parquet. |
| Fraude : liste des sinistres à surveiller et export | ✅ | ✅ | Prédictions déjà calculées par Spark, exportées en Parquet. |
| Fraude : importance des caractéristiques et règles de l'arbre | ✅ | ❌ | Il faut charger le modèle Spark, ce qui demande Spark et Java. |
| Fraude : simulateur d'un sinistre | ✅ | ❌ | Le modèle Spark doit calculer en direct, ce qui demande Spark et Java. |
| Bouton « Recharger depuis Hadoop » | ✅ | ❌ | Il n'y a pas de Hadoop en ligne. |
| État des services : voyants Kafka / Hadoop / Streaming / Spark | ✅ | ⚠️ | La page existe (onglet admin), mais les services sont absents, donc les voyants sont rouges. Voir la section 3. |
| Alerte mail en cas de panne | ✅ | ❌ | Le serveur Mailpit n'existe que dans Docker. |
| Journal des vérifications (`logs/etat_services.log`) | ✅ | ❌ | Sur l'hébergement, les fichiers écrits sont effacés à chaque redémarrage. |
| Performances du pipeline (US 6.3) | ✅ | ❌ | Les chronos portent sur le vrai pipeline, absent en ligne. |
| Arrivée continue des données (streaming Kafka vers HDFS) | ✅ | ❌ | Les données en ligne sont figées à la date d'export. |

✅ = disponible · ❌ = absent de cette version · ⚠️ = présent, mais sans objet à surveiller

### Pages selon le rôle

| Rôle | Onglets visibles |
| --- | --- |
| admin | Tableau de bord, Fraude, Services |
| user | Tableau de bord, Fraude |

Les droits sont définis à un seul endroit, dans `PAGES_PAR_ROLE` (`outils_auth.py`). L'application construit ses onglets à partir de `pages_autorisees(role)`.

---

## 3. L'onglet « Services » en ligne : des voyants rouges attendus

Dans la version en ligne, l'onglet **Services** (réservé à l'admin) affiche :

- **Talend** en gris : Talaxie tourne sur le PC, hors Docker. Il ne peut pas être surveillé dans aucune des deux versions.
- **Kafka, Hadoop et Streaming** en rouge, avec l'alerte « problème sur Kafka, Hadoop, Streaming ».
- **Spark** en rouge (« non installé sur cette version ») ou en vert si pyspark est présent sur la machine.

**C'est le comportement attendu, pas un bug.** Les noms `kafka` et `namenode` n'existent que dans le réseau interne de Docker. Hors de Docker, la machine ne sait pas à quelle adresse les trouver, d'où le message `getaddrinfo failed`. La page détecte cette absence et déclenche l'alerte, ce qui prouve que la surveillance fonctionne.

Deux choix ont été faits pour que la page reste utilisable en ligne :

- **Vérification sur clic** (bouton « Vérifier les services ») : avec `st.tabs`, Streamlit exécute tous les onglets à chaque interaction. Sans bouton, les vérifications ralentiraient tout le tableau de bord.
- **Pas de mail ni de journal** : ces deux fonctions restent dans la version complète (`Etat_des_services.py`).

Les adresses des services sont réglables par variables d'environnement (`HOTE_KAFKA`, `PORT_KAFKA`, `HOTE_NAMENODE`). Depuis le PC, Kafka est joignable sur `localhost:9092`, car ce port est publié par Docker. Le namenode, en revanche, ne publie volontairement pas son port 9870 : HDFS n'est pas exposé sur le réseau, par sécurité. La page Services de la version complète le surveille depuis l'intérieur du réseau Docker, ce qui suffit pour la démonstration.

---

## 4. Préparer la démonstration (version complète, en local)

La démonstration de la vidéo se fait avec la **version complète**, sur le PC.

1. **Lancer la stack Docker** (la commande `docker compose` se lance depuis le dossier `Docker`) :

   ```powershell
   cd Docker
   docker compose up -d
   docker compose ps    # vérifier que tous les conteneurs sont « Up »
   cd ..
   ```

   Conteneurs attendus : `namenode`, `datanode`, `datanode2`, `kafka`, `pyspark-app` (l'application, port 8501) et `mailpit` (port 8025). `kafka-init-permissions` s'arrête tout seul après son travail : c'est normal.
   L'ancien conteneur `abassurance-streamlit` utilise lui aussi le port 8501. Il ne doit pas être démarré en même temps que `pyspark-app`.
2. **S'assurer que Hadoop contient les données** : les 4 tables dans `/data/kafka/`, les tables nettoyées dans `/data/clean/`.
3. **S'assurer que le modèle de fraude existe** dans `/data/models/modele_fraude`. Sinon, lancer dans l'ordre :

   ```powershell
   docker exec -it pyspark-app python src/pipeline/preparer_dataset_fraude.py
   docker exec -it pyspark-app python src/prediction/entrainer_modele_fraude.py
   ```

4. **Ouvrir l'application** : http://localhost:8501, puis se connecter en admin.
5. **Avant d'enregistrer :**
   - ouvrir la page **Détection de fraude** une première fois, car le démarrage de Spark et le chargement du modèle prennent du temps. Ensuite, ils restent en mémoire et le simulateur répond vite ;
   - ouvrir **État des services** et vérifier que tous les voyants (sauf Talend) sont verts ;
   - préparer un ou deux exemples de sinistres à saisir dans le simulateur.
6. **Pour montrer l'alerte** pendant la démo, arrêter un service :

   ```powershell
   docker stop kafka      # le voyant Kafka passe au rouge, un mail arrive dans Mailpit
   docker start kafka     # retour au vert
   ```

   Le mail d'alerte se consulte dans Mailpit : http://localhost:8025.

---

## 5. Mettre à jour les données de la version en ligne

Les fichiers du dossier `en_ligne/donnees` sont des copies des tables Parquet de HDFS. Pour les rafraîchir, la stack Docker doit être lancée. Pour chaque table, exécuter les commandes suivantes depuis le dossier `en_ligne` :

```powershell
# 1) Copier la table de HDFS vers le disque du conteneur namenode
docker exec namenode hdfs dfs -get /data/clean/<table> /tmp/<table>
# 2) Copier du conteneur vers le PC
docker cp namenode:/tmp/<table> ./donnees/<table>
```

Tables utilisées : `clients`, `contrats`, `paiements`, `sinistres`, `predictions_fraude`, `metriques_fraude`.

Ensuite, faire un commit et un push sur Git : Streamlit Community Cloud redéploie l'application automatiquement.

### Tester avec ses propres données (correcteur)

Une fois connecté, la barre latérale propose une zone **« Importer vos données »** :

- Fichiers acceptés : `.csv` (séparateur `;` ou `,`, encodage UTF-8 ou celui d'Excel) et `.parquet`.
- Le **nom du fichier** indique la table remplacée : `clients`, `contrats`, `paiements` ou `sinistres` (majuscules acceptées).
- Chaque fichier remplace la table du même nom **pour la session en cours uniquement**. Les fichiers d'origine ne sont jamais modifiés. Retirer le fichier de la zone de dépôt rétablit les données de démonstration.
- Les colonnes obligatoires sont listées dans la zone « Colonnes attendues ». Un fichier auquel il en manque est refusé avec un message qui les nomme. Les colonnes en plus sont acceptées.

| Table | Colonnes obligatoires |
| --- | --- |
| clients | client_id, code_postal, date_creation, statut_client |
| contrats | statut_contrat, type_assurance |
| paiements | aucune (la table est seulement comptée) |
| sinistres | sinistre_id, contrat_id, date_sinistre, montant_estime, statut_sinistre |

**Limite :** l'onglet Fraude continue d'afficher les prédictions calculées sur les sinistres de démonstration. Recalculer la fraude sur des sinistres importés demande Spark et le modèle, donc la version complète. Un message le signale quand des sinistres ont été importés.

La lecture et la vérification des fichiers sont dans `outils_import.py`, sans dépendance à Streamlit, et couvertes par `test_outils_import.py` (13 tests).

En local, la variable d'environnement `DOSSIER_DONNEES` permet aussi de pointer l'application vers un autre dossier de fichiers Parquet.

---

## 6. Limites assumées

- **Données figées** : la version en ligne ne montre pas les données qui arrivent après l'export (sauf celles importées à la main, le temps d'une session).
- **Pas de réentraînement ni de simulation en ligne** : le modèle n'est utilisable qu'avec Spark, donc dans la version complète.
- **Modèle indicatif** : le jeu d'entraînement ne compte que 37 sinistres (données générées). Les probabilités de fraude sont des alertes à vérifier, pas des certitudes.
- **Supervision partielle** : Talaxie n'est surveillé dans aucune des deux versions, car il tourne hors Docker.