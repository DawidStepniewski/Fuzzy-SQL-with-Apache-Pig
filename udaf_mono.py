from pig_util import outputSchema

@outputSchema("result:chararray")
class Last5Records:
    def __init__(self):
        self.records = []

    def accumulate(self, value):
        self.records.append((float(value)))

    def getValue(self):
            # Sort the records based on timestamps in descending order
            sorted_records = sorted(self.records, key=lambda x: x[1], reverse=True)

            # Take the last 5 records
            last_5_records = sorted_records[:5]

            # Create a string representation of the last 5 records
            result_string = ", ".join(["({}, {})".format(value) for (value) in last_5_records])
            return result_string