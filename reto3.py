from mrjob.job import MRJob
from mrjob.step import MRStep
from statistics import mean

class MovieAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_line,
                   reducer=self.reducer_aggregate_data),
            MRStep(reducer=self.reducer_user_stats),
            MRStep(reducer=self.reducer_movie_stats),
            MRStep(reducer=self.reducer_day_stats)
        ]

    def mapper_parse_line(self, _, line):
        fields = line.strip().split(',')
        if fields[0] == 'Usuario':
            return  # Ignorar la línea de encabezado

        try:
            user, movie, rating, genre, date = fields
            rating = int(rating)
            yield (user, movie, date, genre), rating
            yield user, (movie, rating)
            yield movie, (user, rating)
            yield date, (movie, rating)
            yield genre, (movie, rating)
        except ValueError:
            pass  # Ignorar líneas con datos incorrectos

    def reducer_aggregate_data(self, key, values):
        if isinstance(key, tuple):
            user, movie, date, genre = key
            ratings = list(values)
            yield 'user_movie', (user, movie, date, genre, mean(ratings))
            yield 'date_movie', (date, movie, mean(ratings))
            yield 'genre_movie', (genre, movie, mean(ratings))
        else:
            data = list(values)
            if isinstance(data[0], tuple):
                for val in data:
                    yield key, val
            else:
                yield key, mean(map(float, data))

    def reducer_user_stats(self, key, values):
        if key == 'user_movie':
            for user, movie, date, genre, avg_rating in values:
                yield ('user', user), (1, avg_rating)
        elif key == 'date_movie':
            for date, movie, avg_rating in values:
                yield ('date', date), (1, avg_rating)
        elif key == 'genre_movie':
            genre_movie_ratings = {}
            for genre, movie, avg_rating in values:
                if genre not in genre_movie_ratings:
                    genre_movie_ratings[genre] = []
                genre_movie_ratings[genre].append((movie, avg_rating))
            for genre, movies in genre_movie_ratings.items():
                movies.sort(key=lambda x: x[1])
                yield ('genre_best', genre), movies[-1]
                yield ('genre_worst', genre), movies[0]

    def reducer_movie_stats(self, key, values):
        if key[0] == 'user':
            user = key[1]
            count_ratings = 0
            sum_ratings = 0
            for count, avg_rating in values:
                count_ratings += count
                sum_ratings += avg_rating
            yield 'user_stats', (user, count_ratings, sum_ratings / count_ratings)
        elif key[0] == 'date':
            date = key[1]
            count_ratings = 0
            sum_ratings = 0
            for count, avg_rating in values:
                count_ratings += count
                sum_ratings += avg_rating
            yield 'day_stats', (date, count_ratings, sum_ratings / count_ratings)
        elif key[0] == 'genre_best':
            yield 'best_movie_by_genre', (key[1], values)
        elif key[0] == 'genre_worst':
            yield 'worst_movie_by_genre', (key[1], values)

    def reducer_day_stats(self, key, values):
        if key == 'user_stats':
            for user, count_ratings, avg_rating in values:
                yield 'user_stats', (user, count_ratings, avg_rating)
        elif key == 'day_stats':
            day_data = list(values)
            day_most_movies = max(day_data, key=lambda x: x[1])
            day_least_movies = min(day_data, key=lambda x: x[1])
            day_worst_rating = min(day_data, key=lambda x: x[2])
            day_best_rating = max(day_data, key=lambda x: x[2])
            yield 'day_most_movies', day_most_movies
            yield 'day_least_movies', day_least_movies
            yield 'day_worst_rating', day_worst_rating
            yield 'day_best_rating', day_best_rating
        else:
            for value in values:
                yield key, value

if __name__ == '__main__':
    MovieAnalysis.run()
