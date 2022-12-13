from typing import Optional, Tuple
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from backoff import backoff


class PSExtract:
    LIMIT_ROWS = 100

    def __init__(self, pg_conn: _connection, curs: DictCursor, offset: int) -> None:
        self.pg_conn = pg_conn
        self.curs = curs
        self.offset = offset

    @staticmethod
    @backoff()
    def extract_data(query: str, curs: DictCursor):
        """
        Метод загрузки данных из Postgres

        Parameters
        ----------
        :param query: запрос к БД
        :param curs: курсор Postgres
        ----------
        """
        curs.execute(query)
        data = curs.fetchall()
        return data

    def extract_table_data(self, last_modified: str, table_name: str, model_name: Optional[str]=None) -> list:
        """
        Метод выгрузки информации по фильмам

        Parameters
        ----------
        :param last_modified: дата с которой начинать отбирать записи
        :param table_name: имя таблицы Postgres
        :param model_name: имя модели Postgres

        :return: список словарей со всеми данными по фильмам
        ----------
        """
        match table_name:
            case "film_work":
                return self.extract_filmwork_data(last_modified, model_name)
            case "persons":
                return self.extract_person_data(last_modified)
            case "genres":
                return self.extract_genre_data(last_modified)


    def extract_filmwork_data(self, last_modified: str, model_name: str) -> list:
        """
        Метод выгрузки информации по фильмам

        Parameters
        ----------
        :param last_modified: дата с которой начинать отбирать записи
        :param model_name: имя модели Postgres

        :return: список словарей со всеми данными по фильмам
        ----------
        """
        if model_name == 'film_work':
            where = f"WHERE fw.modified > '{last_modified}' "
        elif model_name == 'person':
            where = f"WHERE p.modified > '{last_modified}' "
        elif model_name == 'genre':
            where = f"WHERE g.modified > '{last_modified}' "
        query = (
            "SELECT fw.id as fw_id, fw.title, fw.description, "
            "fw.rating, fw.type, fw.created, fw.modified, "
            "COALESCE ( \
                json_agg( \
                    DISTINCT jsonb_build_object( \
                        'person_id', p.id, \
                        'role', pfw.role, \
                        'full_name', p.full_name \
                    ) \
                ) FILTER (WHERE p.id is not null), \
                '[]' \
            ) as persons, "
            "array_agg(DISTINCT g.name) as genres "
            "FROM content.film_work fw "
            "LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
            "LEFT JOIN content.person p ON p.id = pfw.person_id "
            "LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
            "LEFT JOIN content.genre g ON g.id = gfw.genre_id "
            f"{where}"
            "GROUP BY fw.id "
            "ORDER BY modified "
            f"LIMIT {self.LIMIT_ROWS} OFFSET {self.offset};"
        )
        data = self.extract_data(query, self.curs)

        return data

    def extract_person_data(self, last_modified: str) -> list:
        """
        Метод выгрузки информации по персонам

        Parameters
        ----------
        :param last_modified: дата с которой начинать отбирать записи

        :return: список словарей с необходимыми данными по персонам
        ----------
        """
        where = f"WHERE p.modified > '{last_modified}' "
        query = (
            "SELECT p.id , p.full_name as name "
            "FROM content.person p "
            f"{where}"
            "GROUP BY p.id "
            "ORDER BY modified "
            f"LIMIT {self.LIMIT_ROWS} OFFSET {self.offset};"
        )
        data = self.extract_data(query, self.curs)
        return data

    def extract_genre_data(self, last_modified: str) -> list:
        """
        Метод выгрузки информации по жанрам

        Parameters
        ----------
        :param last_modified: дата с которой начинать отбирать записи

        :return: список словарей с необходимыми данными по жанрам
        ----------
        """
        where = f"WHERE g.modified > '{last_modified}' "
        query = (
            "SELECT g.id, g.name as genre, g.description "
            "FROM content.genre g "
            f"{where}"
            "GROUP BY g.id "
            "ORDER BY modified "
            f"LIMIT {self.LIMIT_ROWS} OFFSET {self.offset};"
        )
        data = self.extract_data(query, self.curs)

        return data