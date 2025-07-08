# /Backend/PythonWorkers/seer_orchestrator/Dockerfile
FROM python:3.9-slim

# Install curl for health checks
# apt-get update && install && clean up
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*


# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first to leverage Docker's layer caching
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main application script
COPY main.py .

# Expose the port the application will run on and command to run the app
EXPOSE 5001
CMD ["flask", "--app", "main", "run", "--host=0.0.0.0", "--port=5001"]