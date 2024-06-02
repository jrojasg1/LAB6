from mrjob.job import MRJob
from mrjob.step import MRStep

class SalaryStatistics(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_salary,
                   reducer=self.reducer_salary),
            MRStep(mapper=self.mapper_sector,
                   reducer=self.reducer_sector)
        ]

    def mapper_salary(self, _, line):
        # Dividir la línea en campos
        fields = line.strip().split(',')
        emp_id, se_id, salary = fields[0], fields[1], float(fields[2])

        # Emitir para calcular el salario promedio por SE
        yield ('SE', se_id), (salary, 1)

        # Emitir para calcular el salario promedio por Empleado
        yield ('EMP', emp_id), (salary, 1)

    def reducer_salary(self, key, values):
        total_salary = 0
        total_count = 0

        for salary, count in values:
            total_salary += salary
            total_count += count

        yield key, total_salary / total_count

    def mapper_sector(self, key, value):
        if key[0] == 'EMP':
            yield key[1], value
        elif key[0] == 'SE':
            yield key[1], (value, 1)

    def reducer_sector(self, key, values):
        if isinstance(next(values), float):  # For salary average per employee
            yield key, ("Average Salary", next(values))
        else:
            sectors = set()
            for sector, count in values:
                sectors.add(sector)
            yield key, ("Number of SE", len(sectors))

if __name__ == '__main__':
    SalaryStatistics.run()
