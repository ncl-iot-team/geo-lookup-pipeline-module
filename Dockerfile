FROM python:3.9
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN python -m spacy download en_core_web_lg
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "geolocation_identification.py"]