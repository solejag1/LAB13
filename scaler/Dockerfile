FROM python:3.12-slim
# Note: runs as root to access Docker socket (/var/run/docker.sock)
# In production use docker group membership instead
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
CMD ["python", "main.py"]
