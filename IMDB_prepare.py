import pandas as pd
import dask.dataframe as dd
import numpy as np
import sshtunnel
import sqlalchemy as db

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0


# Create basics DF
def prepare():
    movies = pd.read_csv('title.basics.tsv', sep='\t',
                         na_values=r'\N', low_memory=False)
    ratings = pd.read_csv('title.ratings.tsv', sep='\t',
                          na_values=r'\N', low_memory=False)

    docus = movies[movies['genres'].str.contains('Documentary') == True]
    movies = movies.drop(docus.index)
    movies = movies.query('titleType == "movie" or \
                           titleType == "tvMovie" or \
                           titleType == "tvMiniSeries"')
    movies = movies[['tconst', 'primaryTitle', 'originalTitle', 'startYear']]
    movies = pd.merge(movies, ratings, how='left', on=['tconst'])

    int_cols = ['startYear', 'numVotes']
    str_cols = ['tconst', 'primaryTitle', 'originalTitle']
    movies.fillna(-1, inplace=True)
    movies[int_cols] = movies[int_cols].astype(np.int64)
    movies[str_cols] = movies[str_cols].applymap(str)
    movie_list = list(movies['tconst'])

    # Create names DF
    names = dd.read_csv('name.basics.tsv', sep='\t',
                        na_values=r'\N', low_memory=False)
    names = names.compute()
    names = names[['nconst', 'primaryName']]
    names = names.rename(columns={'nconst': 'dconst'})
    names.fillna(-1, inplace=True)

    # Create directors/movies DF
    move_dir = pd.read_csv('title.crew.tsv', sep='\t',
                           na_values=r'\N', low_memory=False)
    move_dir.drop('writers', axis=1, inplace=True)
    move_dir.dropna(inplace=True)
    movie_dir = move_dir.query('tconst in @movie_list').copy()

    movie_dir['directors'] = movie_dir['directors'].str.split(',')
    upper = movie_dir['directors'].str.len().quantile(0.999)
    upper_dirs = movie_dir[movie_dir['directors'].str.len() > upper].index
    movie_dir = movie_dir.drop(upper_dirs)

    movie_dir = pd.concat([movie_dir.reset_index(drop=True),
                           pd.DataFrame(movie_dir['directors'].tolist())],
                          axis=1)
    movie_dir.drop('directors', axis=1, inplace=True)
    movie_dir = movie_dir.melt(id_vars='tconst', value_name='dconst')
    movie_dir.drop('variable', axis=1, inplace=True)
    movie_dir.dropna(inplace=True)

    temp = movie_dir.groupby('dconst')['tconst'].apply(list)
    upper = temp.apply(lambda x: len(x)).quantile(0.999)
    upper_movies = temp[temp.apply(lambda x: len(x)) > upper].index
    to_drop = movie_dir[movie_dir['tconst'].isin(upper_movies)].index
    movie_dir.drop(to_drop, inplace=True)

    directors = movie_dir['dconst'].unique()
    directors = pd.DataFrame(directors, columns=["dconst"])
    directors = pd.merge(directors, names, how='left', on=['dconst'])


    # Create people/movies DF
    movie_prin = dd.read_csv('title.principals.tsv', sep='\t')
    movie_prin = movie_prin.compute()
    movie_prin = movie_prin.query('tconst in @movie_list')
    movie_prin = movie_prin[['tconst', 'nconst']]
    movie_prin = movie_prin.rename(columns={'nconst': 'pconst'})
    movie_prin.dropna(inplace=True)


def post_to_server(directors, movies,
                   movie_dir, movie_prin):
    with sshtunnel.SSHTunnelForwarder(
        ('ssh.pythonanywhere.com'),
        ssh_username='<USERNAME>', ssh_password='<PASSWORD>',
        remote_bind_address=('<USERNAME>.mysql.pythonanywhere-services.com', 3306)
    ) as tunnel:
        engine = db.create_engine('mysql+mysqldb://<USERNAME>:<PASSWORD>@127.0.0.1:{}'
            '/<USERNAME>$IMDB?charset=utf8mb4'.format(tunnel.local_bind_port))
        con = engine.connect()

        directors.to_sql(con=con, name='directors', if_exists='replace',
                         chunksize=5000, index=False)
        movies.to_sql(con=con, name='movies', if_exists='replace',
                      chunksize=5000, index=False)

        movie_dir.to_sql(con=con, name='movie_dir', if_exists='replace',
                         chunksize=5000, index=False)

        movie_prin.to_sql(con=con, name='movie_prin', if_exists='replace',
                          chunksize=5000, index=False)

        con.execute("""CREATE TABLE principles(
                           pconst VARCHAR(10),
                           PRIMARY KEY (pconst));""")

        con.execute("""INSERT INTO principles (pconst)
                       SELECT DISTINCT pconst
                       FROM movie_prin;""")

        con.execute("""ALTER TABLE directors MODIFY dconst VARCHAR(10),
                                             MODIFY primaryName VARCHAR(255),
                                             ADD PRIMARY KEY (dconst);""")

        con.execute("""ALTER TABLE movies MODIFY tconst VARCHAR(10),
                                          MODIFY primaryTitle VARCHAR(255),
                                          MODIFY originalTitle VARCHAR(255),
                                          MODIFY startYear SMALLINT,
                                          MODIFY averageRating FLOAT,
                                          MODIFY numVotes INT,
                                          ADD PRIMARY KEY (tconst);""")

        con.execute("""ALTER TABLE movie_dir MODIFY tconst VARCHAR(10),
                                             MODIFY dconst VARCHAR(10),
                                             ADD FOREIGN KEY (dconst)
                                                 REFERENCES directors (dconst),
                                             ADD FOREIGN KEY (tconst)
                                                 REFERENCES movies (tconst);""")

        con.execute("""ALTER TABLE movie_prin MODIFY tconst VARCHAR(10),
                                              MODIFY pconst VARCHAR(10),
                                              ADD FOREIGN KEY (tconst)
                                                  REFERENCES movies (tconst),
                                              ADD FOREIGN KEY (pconst)
                                                  REFERENCES principles (pconst);
                                              """)

        con.close()


if __name__ == "__main__":
    (directors, movies, movie_dir, movie_prin) = prepare()
    post_to_server(directors, movies, movie_dir, movie_prin)
