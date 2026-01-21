# Base image using the official Airflow image
FROM apache/airflow:2.9.1-python3.11

USER root

# Install Chrome and dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
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

# Insall Git
RUN apt-get update && apt-get install -y git

# Install Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Expose Chrome & ChromeDriver paths to the runtime via environment variables
ENV CHROME_BIN=/usr/bin/google-chrome-stable \
    HEADLESS=true \
    PAGE_LOAD_TIMEOUT=30

# Build-time health check to ensure Chrome & chromedriver are present (fail build early if missing)
RUN if [ ! -x "$CHROME_BIN" ]; then echo "Chrome not found at $CHROME_BIN" >&2; exit 1; fi

COPY airflow/requirements.txt /requirements.txt

USER airflow

# Install Python dependencies
RUN pip install -r /requirements.txt

# Set workdir
WORKDIR /opt/airflow