from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieMetrics(MRJob):

    def mapper(self, _, line):
        data = line.strip().split(",")
        # Filtrar líneas vacías y líneas con encabezados
        if data and data[0] != "Usuario":
            if len(data) == 5:
                user_id, movie_id, rating, genre, date = data
                yield ("user_movie", (user_id, movie_id, float(rating)))
                yield ("date_movie", (date, movie_id, float(rating)))
                yield ("movie_genre", (movie_id, genre, float(rating)))

    def reducer(self, key, values):
        if key == "user_movie":
            movies_count = {}
            users_ratings = {}
            for user_id, movie_id, rating in values:
                movies_count.setdefault(user_id, {"count": 0, "sum": 0})
                movies_count[user_id]["count"] += 1
                movies_count[user_id]["sum"] += rating
                users_ratings.setdefault(movie_id, set())
                users_ratings[movie_id].add(user_id)
            for user_id, data in movies_count.items():
                yield (f"User {user_id}", (f"{data['count']} movies viewed",
                                            f"Average rating: {data['sum'] / data['count']:.2f}"))
            for movie_id, users in users_ratings.items():
                movies_count.setdefault(movie_id, {"count": 0, "sum": 0})  # Inicializar movies_count para cada película
                yield (f"Movie {movie_id}", (f"{len(users)} users viewed",
                                            f"Average rating: {movies_count[movie_id]['sum'] / len(users):.2f}"))
        elif key == "date_movie":
            days = {}
            for date, movie_id, rating in values:
                days.setdefault(date, {"count": 0})
                days[date]["count"] += 1
            sorted_days = sorted(days.items(), key=lambda x: x[1]["count"], reverse=True)
            most_viewed_day, most_views = sorted_days[0]
            least_viewed_day, least_views = sorted_days[-1]
            yield ("Most viewed day", (f"{most_viewed_day}: {most_views['count']} views", None))
            yield ("Least viewed day", (f"{least_viewed_day}: {least_views['count']} views", None))
        elif key == "movie_genre":
            genres = {}
            for movie_id, genre, rating in values:
                genres.setdefault(genre, {"count": 0, "sum": 0})
                genres[genre]["count"] += 1
                genres[genre]["sum"] += rating
            for genre, data in genres.items():
                yield (f"Genre {genre}", (f"{data['count']} movies", f"Average rating: {data['sum'] / data['count']:.2f}"))

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    MovieMetrics.run()
