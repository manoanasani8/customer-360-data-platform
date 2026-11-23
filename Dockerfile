FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY warehouse.py demo.py test_warehouse.py schema.sql marts.sql batch_001.json batch_002.json ./
CMD ["python", "demo.py", "--output", "/app/demo-output"]
