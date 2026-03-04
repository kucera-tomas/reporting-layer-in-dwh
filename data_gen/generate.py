import pandas as pd
import random
from faker import Faker
from datetime import timedelta
from sqlalchemy import create_engine

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# --- CONFIGURATION ---
NUM_CUSTOMERS = 50
NUM_SUBSCRIPTIONS = 70
NUM_TRANSACTIONS = 200

# Simple MySQL connection string
DATABASE_URI = 'mysql+pymysql://root:@localhost:3306/dw_bronze'

def generate_mock_data():
    print("Generating Customers ...")
    customers = []
    customer_ids = [fake.unique.random_int(min=111111, max=999999) for _ in range(NUM_CUSTOMERS)]
    
    for cust_id in customer_ids:
        customers.append({
            'customer_id': cust_id,
            'company_name': fake.company(),
            'country': fake.country(),
            'signup_date': fake.date_between(start_date='-3y', end_date='-1y')
        })
    
    # Customer with future signup date
    customers.append({
        'customer_id': fake.unique.random_int(min=111111, max=999999),
        'company_name': fake.company(),
        'country': fake.country(),
        'signup_date': fake.date_between(start_date='+1m', end_date='+1y')
    })

    # Customer with empty company name
    customers.append({
        'customer_id': fake.unique.random_int(min=111111, max=999999),
        'company_name': '',
        'country': fake.country(),
        'signup_date': fake.date_between(start_date='+1m', end_date='+1y')
    })

    # Customer with null country
    customers.append({
        'customer_id': fake.unique.random_int(min=111111, max=999999),
        'company_name': fake.company(),
        'country': None,
        'signup_date': fake.date_between(start_date='+1m', end_date='+1y')
    })

    # Customer with invalid customer_id (negative number)
    customers.append({
        'customer_id': -fake.unique.random_int(min=111111, max=999999),
        'company_name': fake.company(),
        'country': fake.country(),
        'signup_date': fake.date_between(start_date='+1m', end_date='+1y')
    })

    df_customers = pd.DataFrame(customers)


    print("Generating Subscriptions ...")
    subscriptions = []
    sub_ids = [fake.unique.random_int(min=111111, max=999999) for _ in range(NUM_SUBSCRIPTIONS)]
    
    for i, sub_id in enumerate(sub_ids):
        # Subscriptions with non-existent customer IDs
        if i < 5: 
            c_id = fake.unique.random_int(min=111111, max=999999) # A random ID not in customer_ids
        else:
            c_id = random.choice(customer_ids)
            
        plan_type = random.choice(['Monthly', 'Annual'])
        amount = 100.0 if plan_type == 'Monthly' else 1200.0

        # Negative subscription amount
        if 5 <= i < 10:
            amount = -amount

        start_date = fake.date_between(start_date='-2y', end_date='today')

        # Future start dates    
        if 10 <= i < 15:
           start_date = fake.date_between(start_date='+1m', end_date='+1y')


        # Inverted Dates (end_date before start_date)
        if 15 <= i < 20:
            end_date = start_date - timedelta(days=random.randint(10, 30))
        else:
            # 80% chance of being active (null end date)
            is_active = random.random() < 0.8
            end_date = None if is_active else start_date + timedelta(days=random.randint(30, 365))

            # Active subscriptions with future end dates
            if 20 <= i < 25:
                is_active = True
                end_date = fake.date_between(start_date='+1m', end_date='+1y')

            # Active subscriptions with past dates
            if 25 <= i < 30:
                is_active = True
                end_date = fake.date_between(start_date='-1m', end_date='-1d')

            # Inactive subscriptions with null end dates
            if 30 <= i < 35:
                is_active = False
                end_date = None

        subscriptions.append({
            'sub_id': sub_id,
            'customer_id': c_id,
            'plan_type': plan_type,
            'start_date': start_date,
            'end_date': end_date,
            'amount': amount
        })
    df_subscriptions = pd.DataFrame(subscriptions)

    print("Generating Transactions (with Duplicates and DQ issues)...")
    transactions = []
    
    for i in range(NUM_TRANSACTIONS):
        tx_id = fake.unique.random_int(min=111111, max=999999)
        
        # Transactions mapped to non-existent subscriptions
        if i < 10:
            s_id = fake.unique.random_int(min=111111, max=999999)
        else:
            s_id = random.choice(sub_ids)
            
        tx_date = fake.date_between(start_date='-1y', end_date='today')
        status = random.choices(['Success', 'Failed', 'Refunded'], weights=[0.8, 0.15, 0.05])[0]
        
        # Empty or null status
        if 10 <= i < 20:
            status = random.choice(['-', ''])

        # Future transaction dates
        if 20 <= i < 30:
            tx_date = fake.date_between(start_date='+1m', end_date='+1y')

        transactions.append({
            'tx_id': tx_id,
            'sub_id': s_id,
            'tx_date': tx_date,
            'status': status
        })
        
        # Create Duplicates for the deduplication logic
        if i % 15 == 0: 
            transactions.append({
                'tx_id': tx_id, 
                'sub_id': s_id,
                'tx_date': tx_date - timedelta(hours=2),
                'status': 'Failed'
            })
            
    df_transactions = pd.DataFrame(transactions)

    return df_customers, df_subscriptions, df_transactions

def load_to_mysql(df_customers, df_subscriptions, df_transactions):
    print("Connecting to MySQL...")
    try:
        engine = create_engine(DATABASE_URI)
        
        print("Loading data ...")
        df_customers.to_sql('raw_customers', con=engine, if_exists='replace', index=False)        
        df_subscriptions.to_sql('raw_subscriptions', con=engine, if_exists='replace', index=False)
        df_transactions.to_sql('raw_transactions', con=engine, if_exists='replace', index=False)
        
        print("Data successfully loaded")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    df_c, df_s, df_t = generate_mock_data()
    load_to_mysql(df_c, df_s, df_t)