FROM python:3.10


WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV UWSGI_PROCESSES 1
ENV UWSGI_THREADS 16
ENV UWSGI_HARAKIRI 240
ENV DJANGO_SETTINGS_MODULE 'example.settings'

COPY example/.env .env
COPY docker-entrypoint.sh docker-entrypoint.sh
COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip && \
     pip install -r requirements.txt --no-cache-dir

COPY . .

EXPOSE 8000

RUN chmod +x /opt/app/docker-entrypoint.sh

ENTRYPOINT ["/opt/app/docker-entrypoint.sh"]