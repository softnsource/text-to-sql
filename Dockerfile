FROM python:3.11-slim-bookworm

# Install base tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl gnupg2 unixodbc-dev build-essential ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Node.js 20 (separate layer for better caching)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x -o nodesource_setup.sh && \
    bash nodesource_setup.sh && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -f nodesource_setup.sh && \
    rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 17 (separate layer)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
        gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > \
        /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 && \
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
