FROM python:3.12-slim-bookworm

ARG WHEEL
ARG PROJECT_ID
ARG SENTRY_DSN

ENV PROJECT_ID=$PROJECT_ID
ENV SENTRY_DSN=$SENTRY_DSN

COPY requirements.txt /tmp/requirements.txt
COPY dist/${WHEEL} /tmp/${WHEEL}
COPY main.py /

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir /tmp/${WHEEL} \
    && rm /tmp/requirements.txt /tmp/${WHEEL}

ENTRYPOINT ["/main.py"]
