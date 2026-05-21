FROM python:3.12-slim
RUN adduser --disabled-password --uid 1001 appuser
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
USER appuser
CMD ["python", "main.py"]
