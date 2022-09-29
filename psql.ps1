# Get a PSQL shell in the database container
docker exec -it (docker compose ps -q) psql -U iro
