# init-environment.ps1
$ErrorActionPreference = "Continue"

Write-Host "=== Initialisation de l'environnement AbAssurance ===" -ForegroundColor Cyan

# Attente active de Kafka au lieu d'un délai fixe : on retente jusqu'à
# ce que Kafka réponde, au lieu de espérer qu'il soit prêt après 10 secondes
Write-Host "`nAttente de la disponibilite de Kafka..." -ForegroundColor Yellow
$kafkaPret = $false
$tentatives = 0
while (-not $kafkaPret -and $tentatives -lt 20) {
    $resultat = docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>&1
    if ($LASTEXITCODE -eq 0) {
        $kafkaPret = $true
        Write-Host "Kafka est pret."
    } else {
        $tentatives++
        Write-Host "Kafka pas encore pret (tentative $tentatives/20)... "
        Start-Sleep -Seconds 5
    }
}

if (-not $kafkaPret) {
    Write-Host "ERREUR : Kafka n'a pas repondu apres 100 secondes. Verifie 'docker logs kafka'." -ForegroundColor Red
    exit 1
}

# Création des topics
$topics = @(
    "abassurance.clients.v1",
    "abassurance.contrats.v1",
    "abassurance.paiements.v1",
    "abassurance.sinistres.v1"
)

Write-Host "`nCreation des topics Kafka..." -ForegroundColor Yellow
foreach ($topic in $topics) {
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
        --topic $topic --bootstrap-server localhost:9092 `
        --partitions 1 --replication-factor 1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERREUR lors de la creation de $topic (code $LASTEXITCODE)" -ForegroundColor Red
    } else {
        Write-Host "  -> $topic OK"
    }
}

Write-Host "`nTopics presents sur le cluster :" -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Arborescence HDFS
Write-Host "`nCreation de l'arborescence HDFS..." -ForegroundColor Yellow
$dossiersHdfs = @(
    "/data/kafka/clients", "/data/kafka/contrats",
    "/data/kafka/paiements", "/data/kafka/sinistres",
    "/data/clean",
    "/data/checkpoints/clients", "/data/checkpoints/contrats",
    "/data/checkpoints/paiements", "/data/checkpoints/sinistres"
)
foreach ($dossier in $dossiersHdfs) {
    docker exec namenode hdfs dfs -mkdir -p $dossier
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERREUR pour $dossier (code $LASTEXITCODE)" -ForegroundColor Red
    } else {
        Write-Host "  -> $dossier OK"
    }
}

Write-Host "`n=== Initialisation terminee ===" -ForegroundColor Green
Write-Host "Relancer les jobs Talaxie pour republier les donnees dans Kafka."