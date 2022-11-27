from dc import Filmwork, Genre, Person, GenreFilmWork, PersonFilmWork


class SQLiteExtractor:
    def __init__(self, connection) -> None:
        self.curs = connection.cursor()
        self.dc_map = {
            'film_work': Filmwork,
            'genre': Genre,
            'person': Person,
            'genre_film_work': GenreFilmWork,
            'person_film_work': PersonFilmWork,
        }
        self.page_size = 1000

    def fetch_batch_data(self, table_name: str) -> list:
        sql = f'SELECT * FROM {table_name};'
        self.curs.execute(sql)

        columns = [data[0] for data in self.curs.description]

        modif_columns = {'created_at': 'created', 'updated_at': 'modified'}
        for col in modif_columns.keys():
            if col in columns:
                columns[columns.index(col)] = modif_columns[col]

        while True:
            fetch_rows = [dict(zip(columns, row)) for row in self.curs.fetchmany(self.page_size)]
            if fetch_rows:
                yield [self.dc_map[table_name](**row) for row in fetch_rows]
            else:
                break
