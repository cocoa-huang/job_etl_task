FROM python:3.9

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project files
COPY . .

# Set Python path to include the project directory
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Command to run when container starts - modified to use the correct paths
CMD ["sh", "-c", "cd /app && python query.py"]
