from faker import Faker
import random
import json
import os

fake = Faker('en_NZ')

def generate_customers(n=100):
    """
    Generate n rows of fake banking customer data.
    Returns a list of dictionaries, each representing a banking customer with a unique customer_id.
    """
    customers = []
    for _ in range(n):
        customer = {
            'customer_id': fake.uuid4(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone_number': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
            'created_at': fake.date_time_this_decade().isoformat()
        }
        customers.append(customer)
    return customers


def generate_accounts(customer_id, n=1):
    """
    Generate n fake banking accounts for a given customer_id.
    Returns a list of dictionaries, each representing a banking account.
    """
    accounts = []
    for _ in range(n):
        account = {
            'account_id': fake.uuid4(),
            'customer_id': customer_id,
            'account_number': fake.bban(),
            'account_type': random.choice(['checking', 'savings', 'business', 'student']),
            'bank_name': 'New Zealand Bank',
            'branch': fake.city(),
            'balance': round(random.uniform(100.0, 100000.0), 2),
            'currency': 'NZD',  # Fixed currency to NZD'
            'opened_at': fake.date_time_this_decade().isoformat(),
            'is_active': random.choice([True, False])
        }
        accounts.append(account)
    return accounts


def generate_banking_transactions(account_id, n=1000):
    """
    Generate n rows of fake banking transaction data for a given account_id.
    Returns a list of dictionaries, each representing a transaction.
    """
    transactions = []
    for _ in range(n):
        transaction = {
            'transaction_id': fake.uuid4(),
            'account_id': account_id,
            'transaction_date': fake.date_time_this_year().isoformat(),
            'transaction_type': random.choice(['deposit', 'withdrawal', 'transfer', 'payment']),
            'amount': round(random.uniform(10.0, 5000.0), 2),
            'currency': 'NZD', # Fixed currency to NZD
            'description': fake.sentence(nb_words=6),
            'merchant': fake.company(),
            'category': random.choice(['groceries', 'utilities', 'salary', 'entertainment', 'travel', 'other'])
        }
        transactions.append(transaction)
    return transactions

# Generate fake data for demonstration
customers = generate_customers(42)

# Generate accounts for each customer
accounts = []
for customer in customers:
    # Randomly generate between 1 to 3 accounts per customer
    accounts += generate_accounts(customer['customer_id'], random.randint(1, 3))

# Generate transactions for each account
transactions = []
for account in accounts:
    # Randomly generate between 1 to 99 transactions per account
    transactions += generate_banking_transactions(account['account_id'], random.randint(1, 99))

# Save the generated data to JSON files
parent_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(parent_dir, 'customers.json'), 'w', encoding='utf-8') as f:
    json.dump(customers, f, ensure_ascii=False, indent=2)
with open(os.path.join(parent_dir, 'accounts.json'), 'w', encoding='utf-8') as f:
    json.dump(accounts, f, ensure_ascii=False, indent=2)
with open(os.path.join(parent_dir, 'transactions.json'), 'w', encoding='utf-8') as f:
    json.dump(transactions, f, ensure_ascii=False, indent=2)
