FROM python:3.9-alpine

RUN  pip3 install tweepy google-cloud


COPY streams_producer.py /
EXPOSE	5000
WORKDIR /
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=UTF-8

CMD	["python3", "streams_producer.py"]