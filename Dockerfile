FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    unzip \
    xvfb \
    libxi6 \
    libgconf-2-4 \
    openjdk-11-jdk \
    procps \
    gcc \
    python3-dev \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver
RUN CHROME_VERSION=$(google-chrome --version | awk '{ print $3 }' | cut -d'.' -f1) \
    && CHROMEDRIVER_VERSION=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION") \
    && wget -q -O /tmp/chromedriver.zip "https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip" \
    && unzip /tmp/chromedriver.zip -d /usr/local/bin/ \
    && chmod +x /usr/local/bin/chromedriver \
    && rm /tmp/chromedriver.zip

# Set up Java environment for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Create necessary directories
RUN mkdir -p /app/logs /app/data_crawled /app/data_json /app/processed_markers /app/archive

# Install Python dependencies in logical groups to isolate issues
COPY requirements-base.txt .
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements-base.txt

COPY requirements-data.txt .
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements-data.txt

COPY requirements-streaming.txt .
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements-streaming.txt

COPY requirements-scraping.txt .
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements-scraping.txt

COPY requirements-spark.txt .
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements-spark.txt

# Copy project files
COPY . .

# Make the scripts executable
RUN chmod +x /app/scripts/*.py

# Set environment variables
ENV PYTHONPATH=/app:$PYTHONPATH
ENV HDFS_URL=http://namenode:9870
ENV HDFS_USER=hadoop
ENV PG_HOST=postgres
ENV PG_PORT=5432
ENV PG_DATABASE=car_warehouse
ENV PG_USER=postgres
ENV PG_PASSWORD=postgres

# Default command
CMD ["python", "-m", "scripts.crawl"]