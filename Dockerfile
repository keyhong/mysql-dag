ARG PYTHON_VERSION=3.10

FROM python:${PYTHON_VERSION}-buster

RUN pip install poetry

COPY . .

RUN poetry install