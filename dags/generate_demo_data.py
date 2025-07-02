"""
DAG to generate demo banking data and insert directly into Postgres using the 'pg_demo' Airflow connection.
"""
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from faker import Faker
import random
import json

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Teutenberg", "retries": 1},
    tags=["demo", "data", "postgres"],
)
def generate_demo_data():
    fake = Faker('en_NZ')

    @task()
    def generate_customers(n=42):
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

    @task()
    def generate_accounts(customers, min_accounts=1, max_accounts=3):
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
        return accounts

    @task()
    def generate_transactions(accounts, min_tx=1, max_tx=99):
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
        return transactions

    @task()
    def insert_to_postgres(customers, accounts, transactions):
        hook = PostgresHook(postgres_conn_id='pg_demo')
        # Insert customers
        hook.run("TRUNCATE TABLE RAW.CUSTOMER;")
        for c in customers:
            hook.run(
                "INSERT INTO RAW.CUSTOMER (DATA) VALUES (%s);",
                parameters=[json.dumps(c)]
            )
        # Insert accounts
        hook.run("TRUNCATE TABLE RAW.ACCOUNT;")
        for a in accounts:
            hook.run(
                "INSERT INTO RAW.ACCOUNT (DATA) VALUES (%s);",
                parameters=[json.dumps(a)]
            )
        # Insert transactions
        hook.run("TRUNCATE TABLE RAW.TRANSACTION;")
        for t in transactions:
            hook.run(
                "INSERT INTO RAW.TRANSACTION (DATA) VALUES (%s);",
                parameters=[json.dumps(t)]
            )

    @task()
    def join_and_count_from_db():
        from collections import defaultdict
        from decimal import Decimal, ROUND_HALF_EVEN
        import json
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id='pg_demo')
        # Fetch data from Postgres
        customers = [json.loads(row[0]) for row in hook.get_records('SELECT DATA FROM RAW.CUSTOMER')]
        accounts = [json.loads(row[0]) for row in hook.get_records('SELECT DATA FROM RAW.ACCOUNT')]
        transactions = [json.loads(row[0]) for row in hook.get_records('SELECT DATA FROM RAW.TRANSACTION')]
        # Build lookup for accounts by customer_id
        accounts_by_customer = defaultdict(list)
        for acc in accounts:
            accounts_by_customer[acc['customer_id']].append(acc)
        # Build lookup for transactions by account_id
        transactions_by_account = defaultdict(list)
        for txn in transactions:
            transactions_by_account[txn['account_id']].append(txn)
        # Prepare result: {customer_name, account_number, transaction_count, transaction_type_totals}
        result = []
        for customer in customers:
            customer_id = customer['customer_id']
            customer_name = f"{customer['first_name']} {customer['last_name']}"
            customer_accounts = accounts_by_customer.get(customer_id, [])
            for acc in customer_accounts:
                account_number = acc['account_number']
                account_id = acc['account_id']
                txns = transactions_by_account.get(account_id, [])
                txn_count = len(txns)
                # Calculate total amount per transaction_type, using Decimal for bankers rounding
                type_totals = defaultdict(Decimal)
                for txn in txns:
                    txn_type = txn['transaction_type']
                    type_totals[txn_type] += Decimal(str(txn['amount']))
                # Round totals to 2 decimal places using bankers rounding
                type_totals_rounded = {k: float(v.quantize(Decimal('0.01'), rounding=ROUND_HALF_EVEN)) for k, v in type_totals.items()}
                type_totals_json = json.dumps(type_totals_rounded, ensure_ascii=False)
                result.append({
                    'customer_name': customer_name,
                    'account_number': account_number,
                    'transaction_count': txn_count,
                    'transaction_type_totals': type_totals_json
                })
        print("[DB] Transaction counts per account for each customer:")
        for row in result:
            print(row)
        return result

    # DAG flow
    customers = generate_customers()
    accounts = generate_accounts(customers)
    transactions = generate_transactions(accounts)
    insert_to_postgres(customers, accounts, transactions)
    join_and_count_from_db()

dag = generate_demo_data()
