from mrjob.job import MRJob
from mrjob.step import MRStep

class StockAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_collect_data,
                   reducer=self.reducer_collect_data),
            MRStep(mapper=self.mapper_find_extremes,
                   reducer=self.reducer_find_extremes),
            MRStep(mapper=self.mapper_stable_stocks,
                   reducer=self.reducer_stable_stocks),
            MRStep(mapper=self.mapper_black_day,
                   reducer=self.reducer_black_day)
        ]

    def mapper_collect_data(self, _, line):
        fields = line.strip().split(',')
        if fields[0] == 'Company':
            return  # Ignorar la línea de encabezado

        try:
            company, price, date = fields
            price = float(price)
            yield company, (price, date)
        except ValueError:
            pass  # Ignorar líneas con datos incorrectos

    def reducer_collect_data(self, company, values):
        prices = list(values)
        prices.sort(key=lambda x: x[1])  # Ordenar por fecha

        min_price = min(prices, key=lambda x: x[0])
        max_price = max(prices, key=lambda x: x[0])
        is_stable = all(prices[i][0] <= prices[i + 1][0] for i in range(len(prices) - 1))

        yield company, (min_price, max_price, is_stable, list(prices))

    def mapper_find_extremes(self, company, data):
        min_price, max_price, is_stable, prices = data
        yield company, (min_price, max_price)
        yield 'stable_check', (company, is_stable)

    def reducer_find_extremes(self, key, values):
        if key == 'stable_check':
            for company, is_stable in values:
                if is_stable:
                    yield 'stable', company
        else:
            yield key, list(values)

    def mapper_stable_stocks(self, key, value):
        if key == 'stable':
            yield 'stable', value
        else:
            for price, date in value:
                yield date, (key, price)

    def reducer_stable_stocks(self, key, values):
        if key == 'stable':
            yield 'stable', list(values)
        else:
            prices = list(values)
            min_price = min(prices, key=lambda x: x[1])
            count_min = sum(1 for price in prices if price[1] == min_price[1])
            yield key, count_min

    def mapper_black_day(self, key, value):
        if key != 'stable':
            yield 'black_day', (key, value)

    def reducer_black_day(self, key, values):
        day_counts = {}
        for date, count in values:
            if date not in day_counts:
                day_counts[date] = 0
            day_counts[date] += count

        black_day = max(day_counts, key=day_counts.get)
        yield 'black_day', black_day

if __name__ == '__main__':
    StockAnalysis.run()
