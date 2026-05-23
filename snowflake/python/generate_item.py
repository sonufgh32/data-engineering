import csv
import random
import string
from datetime import datetime, timedelta

# Define constants for generating random data
ITEM_CLASSES = ["Electronics", "Clothing", "Home", "Books", "Beauty"]
ITEM_CATEGORIES = ["Gadgets", "Apparel", "Furniture", "Literature", "Skincare"]
NUM_TOTAL_RECORDS = 1000
NUM_FILES = 5
RECORDS_PER_FILE = NUM_TOTAL_RECORDS // NUM_FILES

def random_string(length=10):
    """Generate a random alphanumeric string of given length."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def random_date(start_year=2000, end_year=2024):
    """Generate a random date as a string."""
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    random_days = random.randint(0, (end_date - start_date).days)
    return (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')

def generate_records(num_records):
    """Generate a list of records for the table."""
    records = []
    for _ in range(num_records):
        item_id = random_string(8)
        item_desc = f"Item {random_string(5)}"
        start_date = random_date()
        # Ensure END_DATE is always after START_DATE
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = random_date(start_year=start_date_obj.year + 1, end_year=start_date_obj.year + 2)
        price = f"{random.uniform(10, 500):.2f}"  # Random price between $10 and $500
        item_class = random.choice(ITEM_CLASSES)
        item_category = random.choice(ITEM_CATEGORIES)
        records.append([item_id, item_desc, start_date, end_date, price, item_class, item_category])
    return records

# Generate and write records to files
for file_num in range(1, NUM_FILES + 1):
    file_name = f"landing_item_data_{file_num}.csv"
    records = generate_records(RECORDS_PER_FILE)
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(["ITEM_ID", "ITEM_DESC", "START_DATE", "END_DATE", "PRICE", "ITEM_CLASS", "ITEM_CATEGORY"])
        # Write data rows
        writer.writerows(records)
    print(f"File {file_num} generated: {file_name}")

print("Data generation complete.")
