docker-compose exec service python manage.py collectstatic --no-input
docker-compose exec service python manage.py migrate
docker-compose exec service python manage.py makemigrations
docker-compose exec service python manage.py migrate
docker-compose exec service python manage.py createsuperuser
docker-compose exec service python sqlite_to_postgres/load_data.py