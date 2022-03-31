import apache_beam as beam


def split_row(element):
    return element.split(',')


def filtering(record):
    return record[3] == 'Accounts'


p1 = beam.Pipeline()

attendance_count = (

        p1
        | beam.io.ReadFromText('input_data/class1/dept_data.txt')
        | beam.Map(split_row)
        # | beam.Map(lambda record: record.split(','))

        | beam.Filter(filtering)
        # |beam.Filter(lambda record: record[3] == 'Accounts')

        | beam.Map(lambda record: (record[1], 1))
        | beam.CombinePerKey(sum)

        | beam.io.WriteToText('output_data/class1/output_new_final')

)

p1.run()
