FROM python:3.12-slim
RUN adduser --disabled-password --uid 1001 appuser
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Include orchestrator module
COPY ../orchestrator/orchestrator.py ./orchestrator/orchestrator.py
RUN mkdir -p orchestrator && touch orchestrator/__init__.py
USER appuser
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
