import csv
from faker import Faker
import datetime

# first_name, last_name, address, date_of_birth


def generate_data(num_records, header, op_filename):
    fake = Faker()
    with open(op_filename, mode="w", newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=header, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for i in range(num_records):
            fullname = fake.name()
            names = str(fullname).split()
            fname = names[0]
            lname = names[1]

            writer.writerow({
                "first_name": fname,
                "last_name": lname,
                "address": str(fake.address()).replace("\n", " "),
                "date_of_birth": fake.date(pattern="%d-%m-%Y", end_datetime=datetime.date(2010, 1, 1))
            })


if __name__ == "__main__":
    records = 10000
    headers = ["first_name", "last_name", "address", "date_of_birth"]
    generate_data(num_records=records, header=headers, op_filename="people_data_small.csv")
