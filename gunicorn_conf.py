# Gunicorn configuration file
import os

workers = int(os.environ.get("GUNICORN_WORKERS", "1"))
worker_class = "uvicorn.workers.UvicornWorker"
bind = "0.0.0.0:" + os.environ.get("PORT", "8111")
accesslog = "-"
errorlog = "-"
keepalive = 120
