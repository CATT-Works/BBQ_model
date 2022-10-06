FROM python:3.10.6-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY app/ .
ENTRYPOINT ["python", "main.py"]
