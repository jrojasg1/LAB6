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
        if fields[0] == 'idemp':
            # Ignorar la línea de encabezado
            return

        try:
            emp_id, se_id, salary, year = fields
            salary = float(salary)

            # Emitir para calcular el salario promedio por SE
            yield ('SE', se_id), (salary, 1)

            # Emitir para calcular el salario promedio por Empleado
            yield ('EMP', emp_id), (salary, 1)

            # Emitir para contar el número de SE por Empleado
            yield (emp_id, 'SE'), se_id
        except ValueError:
            # Manejar líneas con datos incorrectos
            pass

    def reducer_salary(self, key, values):
        if key[0] in ('SE', 'EMP'):
            total_salary = 0
            total_count = 0
            for salary, count in values:
                total_salary += salary
                total_count += count
            yield key, total_salary / total_count
        else:
            sectors = set(values)
            yield key, len(sectors)

    def mapper_sector(self, key, value):
        if key[0] == 'EMP':
            yield key[1], ("Average Salary", value)
        elif key[0] == 'SE':
            yield key[1], ("Average Salary", value)
        else:
            yield key, ("Number of SE", value)

    def reducer_sector(self, key, values):
        for value_type, value in values:
            yield key, (value_type, value)

if __name__ == '__main__':
    SalaryStatistics.run()
