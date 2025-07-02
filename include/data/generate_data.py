from faker import Faker
import random
import json
import os

fake = Faker('en_NZ')

# Helper function to write JSONL (one object per line)
def save_jsonl(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        for obj in data:
            f.write(json.dumps(obj, ensure_ascii=False) + '\n')

# Function to generate fake banking customer data
# Each customer will have a unique customer_id
def generate_customers_json(customers_file, n=100):
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
    save_jsonl(customers, customers_file)
    print(f"Generated {n} customers and saved to {os.path.basename(customers_file)} (JSONL format).")
    return

# Function to update existing customers' fields in the customers.json file
# This function will randomly select n customers and update their details
def update_customers_json(customers_file, n):
    """
    Update n random existing customers' fields: first_name, last_name, email, phone_number, address.
    """
    with open(customers_file, 'r', encoding='utf-8') as f:
        customers = [json.loads(line) for line in f if line.strip()]
    if n > len(customers):
        n = len(customers)
    to_update = random.sample(customers, n)
    for customer in to_update:
        customer['email'] = fake.email()
        customer['phone_number'] = fake.phone_number()
        customer['address'] = fake.address().replace('\n', ', ')
    save_jsonl(customers, customers_file)
    print(f"Updated {n} customers and saved to {os.path.basename(customers_file)} (JSONL format).")

# Function to generate fake banking accounts for a given customer_id
# Each account will have a unique account_id
def generate_accounts_json(customers_file, accounts_file, min_accounts=1, max_accounts=3):
    """
    Generate random number of fake banking accounts for each customer in customers_file.
    Save all accounts to accounts_file.
    """
    with open(customers_file, 'r', encoding='utf-8') as f:
        customers = [json.loads(line) for line in f if line.strip()]
    accounts = []
    for customer in customers:
        n = random.randint(min_accounts, max_accounts)
        for _ in range(n):
            account = {
                'account_id': fake.uuid4(),
                'customer_id': customer['customer_id'],
                'account_number': fake.bban(),
                'account_type': random.choice(['checking', 'savings', 'business', 'student']),
                'bank_name': 'New Zealand Bank',
                'branch': fake.city(),
                'balance': round(random.uniform(100.0, 100000.0), 2),
                'currency': 'NZD',
                'opened_at': fake.date_time_this_decade().isoformat(),
                'is_active': random.choice([True, False])
            }
            accounts.append(account)
    save_jsonl(accounts, accounts_file)
    print(f"Generated randint({min_accounts}, {max_accounts}) accounts for all customers and saved to {os.path.basename(accounts_file)} (JSONL format).")
    return

# Function to update the 'balance' field for each account in accounts.json based on transactions.json
# This function will read and update the accounts.json file
def update_account_balances(accounts_file, transactions_file):
    """
    Update the 'balance' field for each account in accounts.json based on the sum of its transactions in transactions.json.
    Deposits and salary increase balance, withdrawals and payment decrease balance, transfer and other are ignored.
    """
    with open(accounts_file, 'r', encoding='utf-8') as f:
        accounts = [json.loads(line) for line in f if line.strip()]
    with open(transactions_file, 'r', encoding='utf-8') as f:
        transactions = [json.loads(line) for line in f if line.strip()]
    # Build a mapping from account_id to balance delta
    balance_map = {}
    for txn in transactions:
        acc_id = txn.get('account_id')
        amt = txn.get('amount', 0)
        ttype = txn.get('transaction_type', '').lower()
        if acc_id is None:
            continue
        if ttype in ['deposit', 'salary']:
            balance_map[acc_id] = balance_map.get(acc_id, 0) + amt
        elif ttype in ['withdrawal', 'payment']:
            balance_map[acc_id] = balance_map.get(acc_id, 0) - amt
        # transfer and other types are ignored
    for account in accounts:
        acc_id = account.get('account_id')
        if acc_id in balance_map:
            account['balance'] = round(balance_map[acc_id], 2)
    save_jsonl(accounts, accounts_file)
    print(f"Updated balances for all accounts based on transactions and saved to {os.path.basename(accounts_file)} (JSONL format).")

# Function to generate fake banking transactions for a given account_id
# Each transaction will have a unique transaction_id
def generate_banking_transactions_json(accounts_file, transactions_file, min_tx=1, max_tx=99):
    """
    Generate random number of fake banking transactions for each account in accounts_file.
    Save all transactions to transactions_file.
    """
    with open(accounts_file, 'r', encoding='utf-8') as f:
        accounts = [json.loads(line) for line in f if line.strip()]
    transactions = []
    for account in accounts:
        account_id = account.get('account_id')
        if not account_id:
            continue
        n = random.randint(min_tx, max_tx)
        for _ in range(n):
            transaction = {
                'transaction_id': fake.uuid4(),
                'account_id': account_id,
                'transaction_date': fake.date_time_this_year().isoformat(),
                'transaction_type': random.choice(['deposit', 'withdrawal', 'payment']),
                'amount': round(random.uniform(10.0, 5000.0), 2),
                'currency': 'NZD',
                'description': fake.sentence(nb_words=6),
                'merchant': fake.company(),
                'category': random.choice(['groceries', 'utilities', 'salary', 'entertainment', 'travel', 'other'])
            }
            transactions.append(transaction)
    save_jsonl(transactions, transactions_file)
    print(f"Generated randint({min_tx}, {max_tx}) transactions for all accounts and saved to {os.path.basename(transactions_file)} (JSONL format).")
    return


# Generate fake data for demonstration
if __name__ == "__main__":
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    customers_file = os.path.join(parent_dir, 'customers.jsonl')
    accounts_file = os.path.join(parent_dir, 'accounts.jsonl')
    transactions_file = os.path.join(parent_dir, 'transactions.jsonl')

    if all(os.path.exists(f) for f in [customers_file, accounts_file, transactions_file]):
        print("JSON files exist. Running update functions...")
        update_customers_json(customers_file, 5)
        generate_banking_transactions_json(accounts_file, transactions_file, 1, 99)
        update_account_balances(accounts_file, transactions_file)
    else:
        print("Generating new data files...")
        generate_customers_json(customers_file, 42)
        generate_accounts_json(customers_file, accounts_file, 1, 3)
        generate_banking_transactions_json(accounts_file, transactions_file, 1, 99)
        update_account_balances(accounts_file, transactions_file)

