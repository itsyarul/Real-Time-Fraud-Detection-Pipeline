ARG PYTHON_VERSION

FROM python:${PYTHON_VERSION}-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY apps/producer/requirements.txt .

RUN pip install \
    --no-cache-dir \
    -r requirements.txt

COPY apps/producer/producer.py .

ENTRYPOINT [ "python", "producer.py"]