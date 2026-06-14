FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# MCP server 代码由 docker-compose 挂载到 /app/mcp，不在此 COPY。
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
