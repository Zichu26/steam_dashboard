python -m venv .venv
pip3 install -r requirements.txt
docker-compose up -d
docker-compose down
python -m src.ingestion.steam_producer



export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"