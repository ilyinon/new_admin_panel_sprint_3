FILM_WORKS_QUERY = """
SELECT
    "film_work"."id",
    "film_work"."title",
    "film_work"."description",
    "film_work"."creation_date",
    "film_work"."modified",
    "film_work"."rating",
    "film_work"."type"
FROM "content"."film_work"
WHERE
    "film_work"."modified" > %(modified)s
ORDER BY "film_work"."modified" ASC
"""


ACTORS_QUERY = """
SELECT
    "person"."id",
    "person"."full_name" as "name"
FROM
    "content"."person_film_work"
JOIN
    "content"."person" ON "person_film_work"."person_id" = "person"."id"
WHERE
    "person_film_work"."film_work_id" = %(film_id)s
    AND "person_film_work"."role" = 'actor'
"""

DIRECTORS_QUERY = """
SELECT
    "person"."id",
    "person"."full_name" as "name"
FROM
    "content"."person_film_work"
JOIN
    "content"."person" ON "person_film_work"."person_id" = "person"."id"
WHERE
    "person_film_work"."film_work_id" = %(film_id)s
    AND "person_film_work"."role" = 'director'
    """

WRITERS_QUERY = """
SELECT
    "person"."id",
    "person"."full_name" as "name"
FROM
    "content"."person_film_work"
JOIN
    "content"."person" ON "person_film_work"."person_id" = "person"."id"
WHERE
    "person_film_work"."film_work_id" = %(film_id)s
    AND "person_film_work"."role" = 'writer'
"""

GENRES_QUERY = """
SELECT
    "genre"."id",
    "genre"."name"
FROM
    "content"."genre_film_work"
JOIN
    "content"."genre" ON "genre_film_work"."genre_id" = "genre"."id"
WHERE
    "genre_film_work"."film_work_id" = %(film_id)s
"""

GENRES_LIST_QUERY = """
SELECT
    "genre"."id",
    "genre"."name",
    "genre"."description",
    "genre"."modified"
FROM 
    "content"."genre"
WHERE
    "genre"."modified" > %(modified)s
ORDER BY "genre"."modified" ASC
"""

PERSONS_LIST_QUERY = """
SELECT
    "person"."id",
    "person"."full_name",
    "person"."modified"
FROM 
    "content"."person"
WHERE
    "person"."modified" > %(modified)s
ORDER BY "person"."modified" ASC
"""

# для ненормализованных данных
# PERSONS_LIST_QUERY = """
# SELECT
#     "person"."id",
#     "person"."full_name",
#     "person"."modified",
#     "pfw"."role" as "role",
#     ARRAY_AGG(DISTINCT pfw.film_work_id) AS film_ids
# FROM 
#     "content"."person"
# LEFT JOIN "content"."person_film_work" pfw 
#         ON "pfw"."person_id" = "person"."id"
# GROUP BY
#     "person"."full_name",
#     "person"."id",
#     "person"."modified",
#     "pfw"."role"        
# ORDER BY "person"."modified" ASC
# """