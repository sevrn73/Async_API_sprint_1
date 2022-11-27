FROM python:3.8


WORKDIR /opt/app

COPY .env .env
COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip && \
     pip install -r requirements.txt --no-cache-dir

COPY . .

EXPOSE 8000

ENTRYPOINT ["python", "/opt/app/src/wait_for_db.py"]

