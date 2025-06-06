FROM python:3.10-slim

WORKDIR /app

# Copie les fichiers nécessaires (requirements.txt + scripts)
COPY backend/ /app/

# Installation des dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Lancer le producer
CMD ["python", "app.py"]
