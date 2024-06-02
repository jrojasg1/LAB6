from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieStats(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie_views_and_ratings,
                   reducer=self.reducer_count_views_and_ratings),
            MRStep(reducer=self.reducer_find_max_min_views_day),
            MRStep(mapper=self.mapper_get_movie_genre,
                   reducer=self.reducer_best_worst_genre)
        ]
    
    def mapper_get_movie_views_and_ratings(self, _, line):
        if line.startswith('Usuario'):  # Ignorar la primera línea con los encabezados
            return
        data = line.split(',')
        if len(data) != 5:
            return
        user_id, movie_id, rating, genre, date = data
        yield date, (1, float(rating))
        
    def reducer_count_views_and_ratings(self, key, values):
        total_views = 0
        total_rating = 0
        for view, rating in values:
            total_views += view
            total_rating += rating
        yield key, (total_views, total_rating / total_views)
        
    def reducer_find_max_min_views_day(self, key, values):
        max_views_day = None
        min_views_day = None
        max_views = 0
        min_views = float('inf')
        for views, _ in values:
            if views > max_views:
                max_views = views
                max_views_day = key
            if views < min_views:
                min_views = views
                min_views_day = key
        yield "Most viewed day:", max_views_day
        yield "Least viewed day:", min_views_day
    
    def mapper_get_movie_genre(self, _, line):
        if line.startswith('Usuario'):  # Ignorar la primera línea con los encabezados
            return
        parts = line.split(',')
        if len(parts) != 5:
            return
        _, _, rating, genre, date = parts
        yield genre, float(rating)
    
    def reducer_best_worst_genre(self, key, values):
        ratings = list(values)
        avg_rating = sum(ratings) / len(ratings)
        yield f"Average rating for {key} genre:", avg_rating

if __name__ == '__main__':
    MovieStats.run()
