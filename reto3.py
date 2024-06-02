from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieMetrics(MRJob):

    def mapper_user_movie_count(self, _, line):
        if isinstance(line, str):
            data = line.strip().split(",")
            if data and data[0] != "Usuario":
                user_id, movie_id, rating, genre, date = data
                yield user_id, (1, int(rating))

    def reducer_user_movie_count(self, user_id, counts):
        total_movies = 0
        total_rating = 0
        count = 0
        for movie_count, rating in counts:
            total_movies += 1
            total_rating += rating
            count += movie_count
        yield "Número de películas vista por un usuario", (user_id, total_movies, total_rating / count)

    def mapper_date_movie_count(self, _, line):
        if isinstance(line, str):
            data = line.strip().split(",")
            if data and data[0] != "Usuario":
                user_id, movie_id, rating, genre, date = data
                yield date, 1

    def reducer_date_movie_count(self, date, counts):
        total = sum(counts)
        yield None, ("Día en que más películas se han visto", (date, total))
        yield None, ("Día en que menos películas se han visto", (date, total))

    def mapper_movie_user_count(self, _, line):
        if isinstance(line, str):
            data = line.strip().split(",")
            if data and data[0] != "Usuario":
                user_id, movie_id, rating, genre, date = data
                yield movie_id, (user_id, int(rating))

    def reducer_movie_user_count(self, movie_id, user_ratings):
        users = {}
        total_rating = 0
        count = 0
        for user_id, rating in user_ratings:
            users[user_id] = True
            total_rating += rating
            count += 1
        yield "Número de usuarios que ven una misma película y el rating promedio", (movie_id, len(users), total_rating / count)

    def mapper_date_average_rating(self, _, line):
        if isinstance(line, str):
            data = line.strip().split(",")
            if data and data[0] != "Usuario":
                user_id, movie_id, rating, genre, date = data
                yield date, int(rating)

    def reducer_date_average_rating(self, date, ratings):
        ratings_list = list(ratings)
        average_rating = sum(ratings_list) / len(ratings_list)
        yield None, ("Día en que peor evaluación en promedio han dado los usuarios", (date, average_rating))
        yield None, ("Día en que mejor evaluación han dado los usuarios", (date, average_rating))

    def mapper_genre_rating(self, _, line):
        if isinstance(line, str):
            data = line.strip().split(",")
            if data and data[0] != "Usuario":
                user_id, movie_id, rating, genre, date = data
                yield genre, int(rating)

    def reducer_genre_rating(self, genre, ratings):
        ratings_list = list(ratings)
        yield f"La mejor película evaluada en {genre}", max(ratings_list)
        yield f"La peor película evaluada en {genre}", min(ratings_list)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_user_movie_count, reducer=self.reducer_user_movie_count),
            MRStep(mapper=self.mapper_date_movie_count, reducer=self.reducer_date_movie_count),
            MRStep(mapper=self.mapper_movie_user_count, reducer=self.reducer_movie_user_count),
            MRStep(mapper=self.mapper_date_average_rating, reducer=self.reducer_date_average_rating),
            MRStep(mapper=self.mapper_genre_rating, reducer=self.reducer_genre_rating)
        ]

if __name__ == '__main__':
    MovieMetrics.run()
