#!/bin/sh

exec python manage.py collectstatic --no-input & python manage.py wait_for_db