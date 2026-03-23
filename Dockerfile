FROM python:3.11-slim-bookworm

# Install system deps: Node.js, ODBC driver, build tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl gnupg2 unixodbc-dev build-essential && \
    # Node.js 20
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    # Microsoft ODBC Driver 17
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
        tee /etc/apt/trusted.gpg.d/microsoft.asc && \
    curl https://packages.microsoft.com/config/debian/12/prod.list > \
        /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Build frontend first (cache layer)
COPY frontend/ frontend/
RUN cd frontend && npm install && npm run build

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Render sets PORT env var (default 10000)
ENV PORT=10000
EXPOSE 10000

CMD gunicorn app.main:app -w 2 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT --timeout 300
