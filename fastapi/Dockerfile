FROM python:3.8


WORKDIR /opt/fastapi

COPY .env .env
COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip && \
     pip install -r requirements.txt --no-cache-dir

COPY . .

EXPOSE 8001

ENTRYPOINT ["python", "/opt/fastapi/src/wait_for_db.py"]

