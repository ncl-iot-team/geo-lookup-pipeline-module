FROM python:alpine3.7
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "Kafka-stream-process.py"]