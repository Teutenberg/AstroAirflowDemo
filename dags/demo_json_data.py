"""
DAG to join customers, accounts, and transactions data and count the number of transactions per account for each customer.
"""
from airflow.decorators import dag, task
from pendulum import datetime
import json
import os
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_EVEN, getcontext

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Teutenberg", "retries": 1},
    tags=["demo", "data", "json"],
)
def demo_json_data():
    @task()
    def load_data():
        data_dir = 'include/data'
        with open(os.path.join(data_dir, 'customers.json'), encoding='utf-8') as f:
            customers = json.load(f)
        with open(os.path.join(data_dir, 'accounts.json'), encoding='utf-8') as f:
            accounts = json.load(f)
        with open(os.path.join(data_dir, 'transactions.json'), encoding='utf-8') as f:
            transactions = json.load(f)
        return {'customers': customers, 'accounts': accounts, 'transactions': transactions}

    @task()
    def join_and_count(data):
        customers = data['customers']
        accounts = data['accounts']
        transactions = data['transactions']

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
                    # Convert to Decimal for precise addition
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
        print("Transaction counts per account for each customer:")
        for row in result:
            print(row)
        return result

    join_and_count(load_data())

demo_json_data()
