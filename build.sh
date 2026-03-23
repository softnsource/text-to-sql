#!/bin/bash

# Install Microsoft ODBC Driver 17 for SQL Server on Linux
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev

# Install Python dependencies
cd frontend && npm install && npm run build && cd .. && pip install -r requirements.txt