python -m venv .venv
pip3 install -r requirements.txt
docker-compose up -d
docker-compose down
python -m src.ingestion.steam_producer