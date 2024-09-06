FROM python:3.9-slim
RUN apt-get update && apt-get install -y libpq-dev gcc
WORKDIR /app
COPY . .
RUN pip install requests psycopg2
CMD ["python", "main.py"]
