CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    certificate TEXT,
    file_path TEXT,
    rating FLOAT,
    type TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid,
    film_work_id uuid,
    created timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    person_id uuid,
    film_work_id uuid,
    role TEXT,
    created timestamp with time zone
);

ALTER TABLE IF EXISTS content.genre_film_work
    ADD CONSTRAINT genre_id FOREIGN KEY (genre_id)
    REFERENCES content.genre (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS content.genre_film_work
    ADD CONSTRAINT film_work_id FOREIGN KEY (film_work_id)
    REFERENCES content.film_work (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS content.person_film_work
    ADD CONSTRAINT person_id FOREIGN KEY (person_id)
    REFERENCES content.person (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS content.person_film_work
    ADD CONSTRAINT film_work_id FOREIGN KEY (film_work_id)
    REFERENCES content.film_work (id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

CREATE UNIQUE INDEX genre_film_work_idx ON content.genre_film_work (genre_id, film_work_id);

CREATE INDEX person_film_work_idx ON content.person_film_work (person_id, film_work_id);

CREATE INDEX film_work_creation_date_idx ON content.film_work (creation_date);