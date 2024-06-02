from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieMetrics(MRJob):

    def mapper(self, _, line):
        data = line.strip().split(",")
        # Verificar si la línea contiene datos válidos y no encabezados
        if len(data) == 5 and data[0] != "User":
            user_id, movie_id, rating, genre, date = data
            yield ("user_movie", (user_id, movie_id, rating))
            yield ("date_movie", (date, movie_id, rating))

    def reducer(self, key, values):
        if key == "user_movie":
            movies_count = {}
            users_ratings = {}
            for user_id, movie_id, rating in values:
                movies_count.setdefault(movie_id, {"count": 0, "sum": 0})
                movies_count[movie_id]["count"] += 1
                movies_count[movie_id]["sum"] += int(rating)
                users_ratings.setdefault(movie_id, set())
                users_ratings[movie_id].add(user_id)
            for movie_id, data in movies_count.items():
                yield None, (f"Movie {movie_id}: {data['count']} views",
                             f"Average rating: {data['sum'] / data['count']:.2f}")
            for movie_id, users in users_ratings.items():
                yield None, (f"Movie {movie_id}: {len(users)} users",
                             f"Average rating: {movies_count[movie_id]['sum'] / len(users):.2f}")
        elif key == "date_movie":
            days = {}
            for date, movie_id, rating in values:
                days.setdefault(date, {"count": 0})
                days[date]["count"] += 1
            for date, data in days.items():
                yield None, (f"Day {date}: {data['count']} views", None)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    MovieMetrics.run()
