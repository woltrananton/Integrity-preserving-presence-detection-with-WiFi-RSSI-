# =====================================================================
# ML inference-container for IoT-Projekt Grupp 12
# =====================================================================
# Anvander Debian/glibc-base sa scikit-learn-wheels installeras direkt
# (musllinux-wheels for sklearn finns inte for cp312-aarch64 — det ar
# darfor vi inte kan kora ML i AppDaemon-add-on direkt).
# =====================================================================

FROM python:3.12-slim-bookworm

WORKDIR /app

# Installera Python-paket (tar ~3 min pa Pi 4 vid forsta build)
RUN pip install --no-cache-dir \
    fastapi==0.115.0 \
    "uvicorn[standard]==0.32.0" \
    scikit-learn==1.5.2 \
    numpy==2.0.2 \
    joblib==1.4.2

# Kopiera in app-koden (montas annars via bind-mount, se docker-compose.yml)
COPY ml_server.py /app/ml_server.py

EXPOSE 8765

CMD ["uvicorn", "ml_server:app", "--host", "0.0.0.0", "--port", "8765"]
