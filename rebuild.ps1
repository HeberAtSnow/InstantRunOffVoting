# Destroy the current database and start up a new one from scratch
docker compose down -v
remove-item .\iro_data\ -Recurse -Force -ErrorAction SilentlyContinue
docker compose up -d
docker compose logs -f
