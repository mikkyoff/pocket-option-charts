FROM python:3.11-slim

# Install Rust toolchain + system dependencies needed to build the PO library
RUN apt-get update && apt-get install -y \
    curl build-essential pkg-config libssl-dev \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && rm -rf /var/lib/apt/lists/*

# Make cargo/rustc available
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Copy requirements first (leverages Docker layer caching)
COPY requirements.txt .

# Install Python dependencies (including the PO library from GitLab)
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port Railway expects
EXPOSE 8080

# Start the Flask app with Gunicorn
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--timeout", "120"]
