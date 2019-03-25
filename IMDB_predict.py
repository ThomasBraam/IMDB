import sqlalchemy as db
import os

DATABASE_KEY = "<WRITE YOUR DATABASE KEY HERE>"


def dir_recomm(DOI_dconst, minimum_rating,
               minimum_votes, dir_limit=5):
    try:
        engine = db.create_engine('mysql+mysqldb://<USERNAME>:{}'
            '@<USERNAME>.mysql.pythonanywhere-services.com:3306/'
            '<USERNAME>$IMDB?charset=utf8mb4'.format(DATABASE_KEY))
        conn = engine.connect()

        DOI_dconst_query = """SELECT primaryName
                              FROM directors
                              WHERE dconst = '{d}'
                              LIMIT 1""".format(d=DOI_dconst)

        DOI_name = conn.execute(DOI_dconst_query).fetchone()[0]

        # find the movies that the director directed,
        # then find the poeple that were in those movies and count how often the DOI
        # worked with those people (count)
        colleague_query = """SELECT mp2.pconst, COUNT(mp2.pconst) AS count
                             FROM movie_prin AS mp2
                             WHERE mp2.tconst IN (
                                 SELECT md1.tconst
                                 FROM movie_dir AS md1
                                 WHERE md1.dconst = '{d}')
                             GROUP BY mp2.pconst""".format(d=DOI_dconst)

        # find all the movies that those people played in and then sum the points per movie
        # (in case multiple people from the previous query worked in that movie).
        col_movie_query = """SELECT mp4.tconst, SUM(mp3.count) AS sum
                             FROM movie_prin AS mp4
                             INNER JOIN ({q}) AS mp3
                             ON mp3.pconst = mp3.pconst
                             WHERE mp4.pconst IN (mp3.pconst)
                             GROUP BY mp4.tconst""".format(q=colleague_query)

        # find the directors that directed those movies and sum those points.
        dir_points_query = """SELECT md5.dconst, SUM(mp5.sum) as sum,
                                     MAX(numVotes) as votes,
                                     MAX(averageRating) as rating
                              FROM movie_dir AS md5
                              INNER JOIN ({q}) AS mp5
                              ON mp5.tconst = md5.tconst
                              INNER JOIN movies as mo
                              ON mo.tconst = md5.tconst
                              WHERE md5.dconst <> '{d}'
                              GROUP BY md5.dconst""".format(q=col_movie_query,
                                                            d=DOI_dconst)

        # see how many movies these directors directed and divide their points by that number.
        dir_score_query = """SELECT md7.dconst
                             FROM movie_dir AS md7
                             INNER JOIN ({q}) AS md6
                             ON md7.dconst = md6.dconst
                             WHERE md6.votes >= {v}
                               AND md6.rating >= {r}
                             GROUP BY md7.dconst
                             ORDER BY md6.sum / COUNT(md7.dconst)
                                 DESC
                             LIMIT {lim}""".format(q=dir_points_query,
                                                   v=minimum_votes,
                                                   r=minimum_rating,
                                                   lim=dir_limit)
        result = conn.execute(dir_score_query).fetchall()
        directors = [r for r, in result]

        movie_list = []
        for director in directors:

            query1 = """SELECT md1.dconst, md1.tconst
                        FROM movie_dir AS md1
                        WHERE dconst = '{d}'""".format(d=director)

            query2 = """SELECT md2.dconst,
                               m2.tconst,
                               m2.primaryTitle,
                               m2.originalTitle,
                               m2.startYear,
                               m2.averageRating,
                               m2.numVotes
                        FROM movies AS m2
                        INNER JOIN ({q}) AS md2
                        ON m2.tconst = md2.tconst
                        ORDER BY averageRating DESC
                        LIMIT 3""".format(q=query1)

            query3 = """SELECT d3.dconst,
                               d3.primaryName,
                               m3.tconst,
                               m3.primaryTitle,
                               m3.originalTitle,
                               m3.startYear,
                               m3.averageRating,
                               m3.numVotes
                        FROM directors AS d3
                        INNER JOIN ({q}) AS m3
                        ON d3.dconst = m3.dconst""".format(q=query2)
            movie_list.extend(conn.execute(query3).fetchall())

        conn.close()
        return(DOI_name, movie_list)

    except Exception:
        conn.close()
        return("error", "")
