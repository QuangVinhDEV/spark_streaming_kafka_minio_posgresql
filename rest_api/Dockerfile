FROM python:3.10-slim
WORKDIR /app
COPY main.py .
RUN pip install fastapi psycopg2-binary uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]