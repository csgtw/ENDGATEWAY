web: gunicorn app:app
worker: RUNNING_AS_WORKER=1 celery -A celery_worker worker --pool=threads --concurrency=15 --loglevel=info
