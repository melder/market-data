FROM python:3.13.7-alpine3.22

# Install system dependencies
RUN apk add --no-cache \
    build-base \
    curl \
    git \
    bash

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser

WORKDIR /app

# Copy dependency files first for better caching
COPY pyproject.toml ./
COPY src/ ./src/

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Change ownership of the app directory
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set the default command
CMD ["bash"]
