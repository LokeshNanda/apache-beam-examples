import apache_beam as beam


def split_row(element):
    return element.split(',')


p = beam.Pipeline()

input_collection = (
        p
        | "Read from text file" >> beam.io.ReadFromText('input_data/dept_data.txt')
        | "Split rows" >> beam.Map(split_row)
)

accounts_count = (
        input_collection
        | 'Get all Accounts dept persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
        | 'Pair each accounts employee with 1' >> beam.Map(lambda record: ("Accounts, " + record[1], 1))
        | 'Group and sum1' >> beam.CombinePerKey(sum)
    #  | 'Write results for account' >> beam.io.WriteToText('data/Account')
)

hr_count = (
        input_collection
        | 'Get all HR dept persons' >> beam.Filter(lambda record: record[3] == 'HR')
        | 'Pair each hr employee with 1' >> beam.Map(lambda record: ("HR, " +record[1], 1))
        | 'Group and sum' >> beam.CombinePerKey(sum)
    #| 'Write results for hr' >> beam.io.WriteToText('data/HR')
)

output =(
        (accounts_count,hr_count)
        | beam.Flatten()
        | beam.io.WriteToText('output_data/class4/both')
)



p.run()
