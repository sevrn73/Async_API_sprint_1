from p_schemas import ESFilmworkData, FilmworkData, ESPersonData, ESGenreData


def parse_from_postgres_to_es(data: dict):
    film_data = FilmworkData.parse_obj(data)
    actors = {}
    writers = {}
    directors = {}
    for person in film_data.persons:
        if person.role == "actor" and person.person_id not in actors:
            actors[person.person_id] = person.full_name
        elif person.role == "writer" and person.person_id not in writers:
            writers[person.person_id] = person.full_name
        elif person.role == "director" and person.person_id not in directors:
            directors[person.person_id] = person.full_name

    es_data = ESFilmworkData(
        id=film_data.fw_id,
        imdb_rating=film_data.rating,
        genre=film_data.genres,
        title=film_data.title,
        description=film_data.description,
        director=list(directors.values()),
        actors_names=list(actors.values()),
        writers_names=list(writers.values()),
        actors=[{"id": id, "name": full_name} for id, full_name in actors.items()],
        writers=[{"id": id, "name": full_name} for id, full_name in actors.items()],
    )
    return es_data

def parse_persons_postgres_to_es(data:dict):
    person_data = ESPersonData.parse_obj(data)
    return person_data

def parse_genres_postgres_to_es(data:dict):
    genre_data = ESGenreData.parse_obj(data)
    return genre_data