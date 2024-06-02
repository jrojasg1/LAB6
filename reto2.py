from mrjob.job import MRJob
from mrjob.step import MRStep

class StockAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_collect_data,
                   reducer=self.reducer_collect_data),
            MRStep(reducer=self.reducer_find_extremes_and_stable),
            MRStep(reducer=self.reducer_black_day)
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

        yield 'extremes', (company, min_price, max_price)
        yield 'stable_check', (company, is_stable)
        for date, price in prices:
            yield date, (company, price)

    def reducer_find_extremes_and_stable(self, key, values):
        if key == 'extremes':
            for value in values:
                yield 'extremes_result', value
        elif key == 'stable_check':
            stable_companies = [company for company, is_stable in values if is_stable]
            yield 'stable_companies', stable_companies
        else:
            prices = list(values)
            min_price = min(prices, key=lambda x: x[1])
            count_min = sum(1 for company, price in prices if price == min_price[1])
            yield 'black_day_candidate', (key, count_min)

    def reducer_black_day(self, key, values):
        if key == 'black_day_candidate':
            day_counts = {}
            for date, count in values:
                if date not in day_counts:
                    day_counts[date] = 0
                day_counts[date] += count

            black_day = max(day_counts, key=day_counts.get)
            yield 'black_day', black_day
        else:
            for value in values:
                yield key, value

if __name__ == '__main__':
    StockAnalysis.run()
