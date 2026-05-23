import csv
import random
import string
from datetime import datetime, timedelta
import pandas as pd
import os

# Constants
# ITEM_CLASSES = ["Electronics", "Clothing", "Home", "Books", "Beauty"]
# ITEM_CATEGORIES = ["Gadgets", "Apparel", "Furniture", "Literature", "Skincare"]
# SALUTATIONS = ["Mr.", "Ms.", "Mrs.", "Dr.", "Prof."]
# FIRST_NAMES = ["John", "Jane", "Alex", "Chris", "Sam", "Taylor", "Jordan", "Morgan", "Casey", "Riley"]
# LAST_NAMES = ["Smith", "Johnson", "Brown", "Williams", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
# COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "India", "China", "Japan", "Australia", "Brazil"]
STORE_NAMES = ["SuperMart", "QuickShop", "DailyNeeds", "MegaStore", "DiscountDepot"]

NUM_FILES = 5
RECORDS_PER_FILE = 1000

def read_files_to_dataframe(folder_path, file_extension="csv"):
    dataframes = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith(f".{file_extension}"):
            file_path = os.path.join(folder_path, file_name)
            try:
                df = pd.read_csv(file_path)
                dataframes.append(df)
            except Exception as e:
                print(f"Error reading {file_name}: {e}")

    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df

def random_string(length=10):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def random_date_time():
    """Generate a random date and time."""
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)
    random_days = random.randint(0, (end_date - start_date).days)
    random_seconds = random.randint(0, 86400)
    random_datetime = start_date + timedelta(days=random_days, seconds=random_seconds)
    return random_datetime.strftime('%Y-%m-%d'), random_datetime.strftime('%H:%M:%S')

def random_price():
    """Generate a random sale price, discount, coupon, and calculate net amounts."""
    sale_price = round(random.uniform(10, 500), 2)
    discount_amt = round(sale_price * random.uniform(0.05, 0.2), 2)
    coupon_amt = round(sale_price * random.uniform(0.01, 0.1), 2)
    net_paid = round(sale_price - discount_amt - coupon_amt, 2)
    net_paid_tax = round(net_paid * 0.1, 2)  # Assuming 10% tax
    net_profit = round(net_paid - net_paid_tax - (sale_price * 0.6), 2)  # Assuming 60% cost
    return sale_price, discount_amt, coupon_amt, net_paid, net_paid_tax, net_profit

def generate_order_records(num_records, item_data, customer_data):
    """Generate order records linking items and customers."""
    records = []
    for _ in range(num_records):
        order_date, order_time = random_date_time()
        item = item_data.iloc[random.randint(0, len(item_data) - 1)]
        customer = customer_data.iloc[random.randint(0, len(customer_data) - 1)]
        store_id = random_string(6)
        store_name = random.choice(STORE_NAMES)
        order_quantity = random.randint(1, 5)
        sale_price, discount_amt, coupon_amt, net_paid, net_paid_tax, net_profit = random_price()

        records.append([
            order_date,
            order_time,
            item['ITEM_ID'],
            item['ITEM_DESC'],
            customer['CUSTOMER_ID'],
            customer['SALUTATION'],
            customer['FIRST_NAME'],
            customer['LAST_NAME'],
            store_id,
            store_name,
            order_quantity, 
            sale_price,
            discount_amt,
            coupon_amt, 
            net_paid,
            net_paid_tax,
            net_profit
        ])
    return records

# def load_item_data():
#     """Generate mock item data."""
#     return [
#         {"ITEM_ID": random_string(8), "ITEM_DESC": f"Item {random_string(5)}"} for _ in range(1000)
#     ]

# def load_customer_data():
#     """Generate mock customer data."""
#     return [
#         {"CUSTOMER_ID": random_string(8), "SALUTATION": random.choice(SALUTATIONS), 
#          "FIRST_NAME": random.choice(FIRST_NAMES), "LAST_NAME": random.choice(LAST_NAMES)} 
#         for _ in range(1000)
#     ]

# # Main logic
# item_data = load_item_data()
# customer_data = load_customer_data()

for file_num in range(1, NUM_FILES + 1):
    file_name = f"landing_order_data_{file_num}.csv"
    item_data = read_files_to_dataframe("C:\\Users\\sonuf\\Desktop\\Snowflake\\items_datasets")
    customer_data = read_files_to_dataframe("C:\\Users\\sonuf\\Desktop\\Snowflake\\customers_datasets")
    records = generate_order_records(RECORDS_PER_FILE, item_data, customer_data)
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([
            "ORDER_DATE", "ORDER_TIME", "ITEM_ID", "ITEM_DESC", "CUSTOMER_ID",
            "SALUTATION", "FIRST_NAME", "LAST_NAME", "STORE_ID", "STORE_NAME",
            "ORDER_QUANTITY", "SALE_PRICE", "DISCOUNT_AMT", "COUPON_AMT",
            "NET_PAID", "NET_PAID_TAX", "NET_PROFIT"
        ])
        writer.writerows(records)
    print(f"File {file_num} generated: {file_name}")

print("Order data generation complete.")
