import time
import subprocess
import requests
from django.db import connections
from django.db.utils import OperationalError
from django.core.management import BaseCommand


class Command(BaseCommand):
    """Django command to pause execution until db is available"""

    def handle(self, *args, **options):
        self.stdout.write('Waiting for database...')
        db_conn = None
        while not db_conn:
            try:
                db_conn = connections['default']
            except OperationalError:
                self.stdout.write('Database unavailable, waititng 1 second...')
                time.sleep(1)

        self.stdout.write(self.style.SUCCESS('Database available!'))

        p = subprocess.Popen(['python', 'manage.py', 'migrate'])
        p.wait()

        p = subprocess.Popen(['python', 'manage.py', 'makemigrations'])
        p.wait()

        p = subprocess.Popen(['python', 'manage.py', 'migrate'])
        p.wait()

        p = subprocess.Popen(
            ['python', 'manage.py', 'createsuperuser', '--noinput', '--username=admin', '--email=admin@email.com']
        )
        p.wait()

        request_body = """
        {
            "settings": {
                "refresh_interval": "1s",
                "analysis": {
                "filter": {
                    "english_stop": {
                    "type":       "stop",
                    "stopwords":  "_english_"
                    },
                    "english_stemmer": {
                    "type": "stemmer",
                    "language": "english"
                    },
                    "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english"
                    },
                    "russian_stop": {
                    "type":       "stop",
                    "stopwords":  "_russian_"
                    },
                    "russian_stemmer": {
                    "type": "stemmer",
                    "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer"
                    ]
                    }
                }
                }
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                "id": {
                    "type": "keyword"
                },
                "imdb_rating": {
                    "type": "float"
                },
                "genre": {
                    "type": "keyword"
                },
                "title": {
                    "type": "text",
                    "analyzer": "ru_en",
                    "fields": {
                    "raw": {
                        "type":  "keyword"
                    }
                    }
                },
                "description": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "director": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "actors_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "writers_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "actors": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "name": {
                        "type": "text",
                        "analyzer": "ru_en"
                    }
                    }
                },
                "writers": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "name": {
                        "type": "text",
                        "analyzer": "ru_en"
                    }
                    }
                }
                }
            }
            }
        """
        time.sleep(10)
        requests.put(
            url='http://elasticsearch:9200/movies',
            headers={
                'Content-Type': 'application/json',
            },
            data=request_body,
        )
        request_body = """
        {
            "settings": {
                "refresh_interval": "1s",
                "analysis": {
                "filter": {
                    "english_stop": {
                    "type":       "stop",
                    "stopwords":  "_english_"
                    },
                    "english_stemmer": {
                    "type": "stemmer",
                    "language": "english"
                    },
                    "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english"
                    },
                    "russian_stop": {
                    "type":       "stop",
                    "stopwords":  "_russian_"
                    },
                    "russian_stemmer": {
                    "type": "stemmer",
                    "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer",
                        "keyword_lowercase"
                    ]
                    }
                }
                }
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                "id": {
                    "type": "keyword"
                },
                "genre": {
                    "type": "text",
                    "analyzer": "ru_en",
                    "raw": {
                        "type":  "keyword"
                    }
                },
                "description": {
                    "type": "text",
                    "analyzer": "ru_en"
                }
                }
            }
        }
        """
        time.sleep(3)
        requests.put(
            url='http://elasticsearch:9200/genres',
            headers={
                'Content-Type': 'application/json',
            },
            data=request_body,
        )
        request_body = """
        {
            "settings": {
                "refresh_interval": "1s",
                "analysis": {
                "filter": {
                    "english_stop": {
                    "type":       "stop",
                    "stopwords":  "_english_"
                    },
                    "english_stemmer": {
                    "type": "stemmer",
                    "language": "english"
                    },
                    "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english"
                    },
                    "russian_stop": {
                    "type":       "stop",
                    "stopwords":  "_russian_"
                    },
                    "russian_stemmer": {
                    "type": "stemmer",
                    "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer",
                        "keyword_lowercase"
                    ]
                    }
                }
                }
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "name": {
                        "type": "text",
                        "analyzer": "ru_en",
                        "raw": {
                            "type":  "keyword"
                        }
                    }
                }
            }
        }
        """
        time.sleep(3)
        requests.put(
            url='http://elasticsearch:9200/persons',
            headers={
                'Content-Type': 'application/json',
            },
            data=request_body,
        )

        time.sleep(1)
        p = subprocess.Popen(['python', 'sqlite_to_postgres/load_data.py'])
        p.wait()

        subprocess.run(['gunicorn', 'example.wsgi:application', '--workers=2', '--bind', '0.0.0.0:8000'])
