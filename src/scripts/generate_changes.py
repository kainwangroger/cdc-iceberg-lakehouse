"""
Générateur de changements pour la source PostgreSQL.
Simule des INSERT, UPDATE, DELETE sur les tables CDC.
"""

import random
import time
from datetime import datetime, timezone

import psycopg2

random.seed(42)

TABLE_ACTIONS = {
    "customers": ["insert", "update", "update", "update"],
    "orders": ["insert", "insert", "update", "delete"],
    "order_items": ["insert", "insert", "insert"],
    "inventory": ["update", "update"],
}

PRODUCTS = [
    ("SKU-LAP-001", "Laptop Pro 16\"", "Électronique", 1499.99),
    ("SKU-PHO-001", "Smartphone X2", "Électronique", 799.99),
    ("SKU-TAB-001", "Tablette X1", "Électronique", 449.99),
    ("SKU-BOO-001", "Python pour la Data Science", "Livres", 49.99),
    ("SKU-BOO-002", "Machine Learning Avancé", "Livres", 69.99),
    ("SKU-HOM-001", "Enceinte Bluetooth", "Maison", 89.99),
    ("SKU-HOM-002", "Lampe Connectée", "Maison", 39.99),
    ("SKU-SPO-001", "Montre Connectée", "Sport", 249.99),
]

FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"]
LAST_NAMES = ["Martin", "Dupont", "Bernard", "Petit", "Moreau", "Leroy", "Roux", "Fournier"]


def get_connection():
    return psycopg2.connect(
        dbname="source_db",
        user="postgres",
        password="postgres",
        host="localhost",
        port=5432,
    )


def generate_change(conn, table, action):
    cur = conn.cursor()

    if table == "customers":
        if action == "insert":
            name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
            email = f"{name.lower().replace(' ', '.')}{random.randint(1,999)}@example.com"
            tier = random.choice(["bronze", "silver", "gold"])
            cur.execute(
                "INSERT INTO sales.customers (name, email, phone, loyalty_tier) VALUES (%s, %s, %s, %s)",
                (name, email, f"+33-6-{random.randint(10,99)}-{random.randint(10,99)}-{random.randint(10,99)}-{random.randint(10,99)}", tier),
            )
            print(f"  + INSERT customer: {name}")

        elif action == "update":
            cur.execute("SELECT customer_id, name FROM sales.customers ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row:
                cid, name = row
                new_tier = random.choice(["bronze", "silver", "gold"])
                cur.execute(
                    "UPDATE sales.customers SET loyalty_tier = %s, updated_at = NOW() WHERE customer_id = %s",
                    (new_tier, cid),
                )
                print(f"  ~ UPDATE customer {cid}: {name} → tier {new_tier}")

    elif table == "orders":
        if action == "insert":
            cur.execute("SELECT customer_id FROM sales.customers ORDER BY RANDOM() LIMIT 1")
            cust = cur.fetchone()
            if cust:
                total = round(random.uniform(10, 2000), 2)
                method = random.choice(["card", "paypal", "transfer", "crypto"])
                cur.execute(
                    "INSERT INTO sales.orders (customer_id, total_amount, status, payment_method) VALUES (%s, %s, 'pending', %s)",
                    (cust[0], total, method),
                )
                print(f"  + INSERT order: ${total}")

        elif action == "update":
            cur.execute("SELECT order_id FROM sales.orders WHERE status != 'delivered' ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row:
                oid = row[0]
                new_status = random.choice(["shipped", "delivered", "cancelled"])
                cur.execute(
                    "UPDATE sales.orders SET status = %s, updated_at = NOW() WHERE order_id = %s",
                    (new_status, oid),
                )
                print(f"  ~ UPDATE order {oid}: status → {new_status}")

        elif action == "delete":
            cur.execute("SELECT order_id FROM sales.orders WHERE status = 'cancelled' LIMIT 1")
            row = cur.fetchone()
            if row:
                oid = row[0]
                cur.execute("DELETE FROM sales.order_items WHERE order_id = %s", (oid,))
                cur.execute("DELETE FROM sales.orders WHERE order_id = %s", (oid,))
                print(f"  - DELETE order {oid}")

    elif table == "order_items":
        if action == "insert":
            cur.execute("SELECT order_id FROM sales.orders WHERE status = 'pending' ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row:
                oid = row[0]
                sku, name, cat, price = random.choice(PRODUCTS)
                qty = random.randint(1, 3)
                cur.execute(
                    "INSERT INTO sales.order_items (order_id, product_name, quantity, unit_price) VALUES (%s, %s, %s, %s)",
                    (oid, name, qty, price),
                )
                # Update order total
                cur.execute("UPDATE sales.orders SET total_amount = total_amount + %s, updated_at = NOW() WHERE order_id = %s",
                           (price * qty, oid))
                print(f"  + INSERT order_item: order {oid} + {qty}x {name}")

    elif table == "inventory":
        if action == "update":
            sku, name, cat, price = random.choice(PRODUCTS)
            delta = random.randint(-5, 15)
            cur.execute(
                "UPDATE sales.inventory SET stock_quantity = GREATEST(0, stock_quantity + %s), updated_at = NOW() WHERE sku = %s",
                (delta, sku),
            )
            print(f"  ~ UPDATE inventory: {sku} ({name}) stock delta {delta:+d}")

    conn.commit()
    cur.close()


def main():
    print(f"=== CDC Change Generator ===")
    print(f"Press Ctrl+C to stop\n")

    conn = get_connection()
    try:
        while True:
            table = random.choice(list(TABLE_ACTIONS.keys()))
            action = random.choice(TABLE_ACTIONS[table])
            generate_change(conn, table, action)
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\nStopped")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
