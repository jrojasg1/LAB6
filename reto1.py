from mrjob.job import MRJob
from mrjob.step import MRStep

class SalaryStatistics(MRJob):

    def mapper(self, _, line):
        # Dividir la línea en campos
        fields = line.strip().split(',')

        # Emitir (clave, valor) para calcular el salario promedio por Sector Económico (SE)
        yield fields[1], (float(fields[2]), 1)

        # Emitir (clave, valor) para calcular el salario promedio por Empleado
        yield fields[0], (float(fields[2]), 1)

        # Emitir (clave, valor) para contar el número de SE por Empleado
        yield fields[0], fields[1]

    def reducer(self, key, values):
        total_salary = 0
        total_count = 0
        sectors = set()

        # Calcular el salario promedio por Sector Económico (SE) y el salario promedio por Empleado
        for salary, count in values:
            total_salary += salary
            total_count += count

        yield key, (total_salary / total_count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':
    SalaryStatistics.run()
