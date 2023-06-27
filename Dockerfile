FROM python:3-slim

RUN pip install airflow 

WORKDIR /app

COPY . .


