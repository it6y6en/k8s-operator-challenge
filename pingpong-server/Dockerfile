FROM python:3.11-slim

WORKDIR /server

# Install useful network/debug tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    iputils-ping \
    dnsutils \
    netcat-traditional \
    net-tools \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ .

# CMD ["gunicorn", "--workers", "1", "--bind", "0.0.0.0:80", "--access-logfile", "-", "--error-logfile", "-", "main:app"]
CMD ["gunicorn", "--workers", "1", "--bind", "0.0.0.0:80", "--log-level", "warning", "main:app"]