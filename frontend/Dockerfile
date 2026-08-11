# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Instala dependências em um diretório isolado para o stage final
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim

# Metadados da imagem
LABEL maintainer="DAG Manager"
LABEL description="Flask app para upload e gestão de DAGs no DigitalOcean Droplet"

WORKDIR /app

# Copia dependências instaladas do builder
COPY --from=builder /install /usr/local

# Copia código da aplicação e templates
COPY main.py .
COPY templates/ ./templates/

# Cria diretório do usuário root e define permissão
RUN mkdir -p /root/dags && chown root:root /root/dags

# Variáveis de ambiente (podem ser sobrescritas no docker-compose ou runtime)
ENV UPLOAD_FOLDER=/root/dags \
  SECRET_KEY=change-me-in-production \
  PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1

# Expõe a porta padrão do Gunicorn
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/')" || exit 1

# Inicia com Gunicorn (produção) em vez de Flask dev server
CMD ["gunicorn", \
  "--bind", "0.0.0.0:5000", \
  "--workers", "2", \
  "--timeout", "120", \
  "--access-logfile", "-", \
  "--error-logfile", "-", \
  "main:app"]
