import aioredis
import uvicorn
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from api.v1 import films, genres, persons
from core.config import ProjectSettings
from db import elastic, redis

app = FastAPI(
    title=ProjectSettings().PROJECT_NAME,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
    description='Информация о фильмах, жанрах и людях, участвовавших в создании произведения',
    version='1.0.0',
)


@app.on_event('startup')
async def startup():
    redis.redis = await aioredis.create_redis_pool(
        (ProjectSettings().REDIS_HOST, ProjectSettings().REDIS_PORT), minsize=10, maxsize=20
    )
    elastic.es = AsyncElasticsearch(
        hosts=[f'{ProjectSettings().ELASTIC_HOST}:{ProjectSettings().ELASTIC_PORT}'],
        basic_auth=(ProjectSettings().ES_USER, ProjectSettings().ES_PASSWORD),
        verify_certs=False,
    )


@app.on_event('shutdown')
async def shutdown():
    redis.redis.close()
    await redis.redis.wait_closed()
    await elastic.es.close()


app.include_router(films.router, prefix='/api/v1/films', tags=['films'])
app.include_router(genres.router, prefix='/api/v1/genres', tags=['genres'])
app.include_router(persons.router, prefix='/api/v1/persons', tags=['persons'])

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8001,
    )
