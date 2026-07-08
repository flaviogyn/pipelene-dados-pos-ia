# Use python-slim as a base image
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /usr/app

# Copy requirements file first to utilize Docker build cache
COPY requirements.txt .

# Install DBT and its adapters
RUN pip install --no-cache-dir -r requirements.txt

# Force Oracle adapter to use Thin Mode (does not require Oracle Client Libraries)
ENV ORA_PYTHON_DRIVER_TYPE=thin

# Set DBT profiles directory to point to our project directory
ENV DBT_PROFILES_DIR=/usr/app

# Copy the rest of the application
COPY . /usr/app

# Default command to verify installation
CMD ["dbt", "--version"]
