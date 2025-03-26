# Use the official Python image as a base image
FROM python:3.8-slim

# Setting up the working directory
WORKDIR /app

# Copying project files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Setup Environment Variables
ENV PYTHONPATH=/app

# Set the default command
CMD ["python", "scripts/fetch_data_v3.py"]