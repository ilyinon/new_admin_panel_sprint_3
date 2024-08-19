FROM python:3.12.4-slim

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt /opt/app/requirements
RUN  pip install --upgrade pip \
     && pip install --no-cache-dir -r /opt/app/requirements

COPY . .