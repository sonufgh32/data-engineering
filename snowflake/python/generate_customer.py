import csv
import random
import string

# Define constants for generating random data
SALUTATIONS = ["Mr.", "Ms.", "Mrs.", "Dr.", "Prof."]
COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "India", "China", "Japan", "Australia", "Brazil"]
FIRST_NAMES = ["John", "Jane", "Alex", "Chris", "Sam", "Taylor", "Jordan", "Morgan", "Casey", "Riley"]
LAST_NAMES = ["Smith", "Johnson", "Brown", "Williams", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
NUM_FILES = 5  # Number of files to create
RECORDS_PER_FILE = 1000  # Records per file

def random_email(first_name, last_name):
    """Generate a random email address based on first and last name."""
    domains = ["example.com", "mail.com", "test.org", "company.net"]
    return f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"

def generate_customer_records(num_records):
    """Generate a list of customer records for the table."""
    records = []
    for _ in range(num_records):
        customer_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        salutation = random.choice(SALUTATIONS)
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        birth_day = f"{random.randint(1, 28):02d}"  # Ensures valid day for all months
        birth_month = f"{random.randint(1, 12):02d}"
        birth_year = f"{random.randint(1950, 2005)}"
        birth_country = random.choice(COUNTRIES)
        email_address = random_email(first_name, last_name)
        records.append([
            customer_id, salutation, first_name, last_name, 
            birth_day, birth_month, birth_year, birth_country, email_address
        ])
    return records

# Generate and write records to files
for file_num in range(1, NUM_FILES + 1):
    file_name = f"landing_customer_data_{file_num}.csv"
    records = generate_customer_records(RECORDS_PER_FILE)
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow([
            "CUSTOMER_ID", "SALUTATION", "FIRST_NAME", "LAST_NAME",
            "BIRTH_DAY", "BIRTH_MONTH", "BIRTH_YEAR", "BIRTH_COUNTRY", "EMAIL_ADDRESS"
        ])
        # Write data rows
        writer.writerows(records)
    print(f"File {file_num} generated: {file_name}")

print("Customer data generation complete.")
