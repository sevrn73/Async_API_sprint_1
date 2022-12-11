FROM python:3.10


WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV UWSGI_PROCESSES 1
ENV UWSGI_THREADS 16
ENV UWSGI_HARAKIRI 240
ENV DJANGO_SETTINGS_MODULE 'example.settings'

COPY example/.env .env
COPY run_uwsgi.sh run_uwsgi.sh
COPY uwsgi_worker.sh uwsgi_worker.sh
COPY docker-entrypoint.sh docker-entrypoint.sh
COPY requirements.txt requirements.txt
COPY uwsgi/uwsgi.ini uwsgi.ini

RUN  pip install --upgrade pip && \
     pip install -r requirements.txt --no-cache-dir

COPY . .

EXPOSE 8000

RUN chmod +x /opt/app/docker-entrypoint.sh
RUN chmod +x /opt/app/uwsgi_worker.sh

ENTRYPOINT ["/opt/app/docker-entrypoint.sh"]