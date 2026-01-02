# Base image using the official Airflow image
FROM apache/airflow:2.9.1-python3.11

USER root

# Install Chrome, PostgreSQL, supervisord and dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    postgresql \
    postgresql-contrib \
    postgresql-client \
    supervisor \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Install matching ChromeDriver for version 134
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/134.0.6998.165/linux64/chromedriver-linux64.zip \
    && unzip chromedriver-linux64.zip -d /usr/local/bin \
    && mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /usr/local/bin/chromedriver-linux64 chromedriver-linux64.zip

# Expose Chrome & ChromeDriver paths to the runtime via environment variables
ENV CHROME_BIN=/usr/bin/google-chrome-stable \
    CHROMEDRIVER_PATH=/usr/local/bin/chromedriver \
    HEADLESS=true \
    PAGE_LOAD_TIMEOUT=30

# Build-time health check to ensure Chrome & chromedriver are present (fail build early if missing)
RUN if [ ! -x "$CHROME_BIN" ]; then echo "Chrome not found at $CHROME_BIN" >&2; exit 1; fi \
    && if [ ! -x "$CHROMEDRIVER_PATH" ]; then echo "Chromedriver not found at $CHROMEDRIVER_PATH" >&2; exit 1; fi

COPY requirements.txt /requirements.txt

USER airflow

# Install Python dependencies
RUN pip install -r /requirements.txt

# Copy application files
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
COPY --chown=airflow:root config/airflow.cfg /opt/airflow/airflow.cfg
COPY --chown=airflow:root entrypoint/entrypoint_airflow.sh /opt/airflow/entrypoint.sh
COPY --chown=airflow:root start.sh /opt/airflow/start.sh
COPY --chown=root:root supervisord.conf /etc/supervisor/conf.d/supervisord.conf
RUN chmod +x /opt/airflow/entrypoint.sh /opt/airflow/start.sh

# Create PostgreSQL data directory and set permissions
USER root
RUN mkdir -p /var/lib/postgresql/data && \
    chown -R postgres:postgres /var/lib/postgresql && \
    chmod 700 /var/lib/postgresql/data

# Create init script for PostgreSQL
COPY --chown=root:root init-postgres.sh /usr/local/bin/init-postgres.sh
RUN chmod +x /usr/local/bin/init-postgres.sh

# Copy dbt project (if needed)
# COPY --chown=airflow:root dbt/ /opt/airflow/dbt/

# Set workdir
WORKDIR /opt/airflow