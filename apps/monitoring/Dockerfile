ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY apps/monitoring/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY apps/monitoring/push_metrics.py .
COPY apps/monitoring/run_loop.py .

CMD ["python", "run_loop.py"]
