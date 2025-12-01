  How to Start Everything

  cd docker && docker-compose up -d

  cd airflow && docker-compose up -d

  source .venv/bin/activate
  python -m src.ingestion.game_info_producer --mode continuous
  python -m src.ingestion.player_count_producer --interval 5
  python -m src.batch.reviews_producer

  python streaming/load_game_info.py --continuous --interval 90


  streamlit run dashboard/app.py --server.port 8501