-- ══════════════════════════════════════════
-- Initialisation de la source PostgreSQL
-- Tables de démo pour le CDC
-- ══════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS sales;

-- Table des clients
CREATE TABLE sales.customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(32),
    address JSONB,
    loyalty_tier VARCHAR(16) DEFAULT 'bronze',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Table des commandes
CREATE TABLE sales.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES sales.customers(customer_id),
    total_amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(32) DEFAULT 'pending',
    shipping_address JSONB,
    payment_method VARCHAR(32),
    ordered_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Table des lignes de commande
CREATE TABLE sales.order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES sales.orders(order_id),
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table des inventaires
CREATE TABLE sales.inventory (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(32) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(64),
    stock_quantity INT DEFAULT 0,
    reorder_level INT DEFAULT 10,
    unit_price DECIMAL(10,2),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Seed data
INSERT INTO sales.customers (name, email, phone, loyalty_tier) VALUES
    ('Alice Martin', 'alice@example.com', '+33-6-12-34-56-78', 'gold'),
    ('Bob Dupont', 'bob@example.com', '+33-6-98-76-54-32', 'silver'),
    ('Charlie Bernard', 'charlie@example.com', '+33-7-11-22-33-44', 'bronze'),
    ('Diana Petit', 'diana@example.com', '+33-6-55-44-33-22', 'gold'),
    ('Eve Moreau', 'eve@example.com', '+33-7-66-77-88-99', 'silver');

INSERT INTO sales.inventory (sku, product_name, category, stock_quantity, reorder_level, unit_price) VALUES
    ('SKU-LAP-001', 'Laptop Pro 16"', 'Électronique', 50, 10, 1499.99),
    ('SKU-PHO-001', 'Smartphone X2', 'Électronique', 120, 20, 799.99),
    ('SKU-BOO-001', 'Python pour la Data Science', 'Livres', 200, 30, 49.99),
    ('SKU-HOM-001', 'Enceinte Bluetooth', 'Maison', 80, 15, 89.99),
    ('SKU-SPO-001', 'Montre Connectée', 'Sport', 45, 10, 249.99);

INSERT INTO sales.orders (customer_id, total_amount, status, payment_method) VALUES
    (1, 1499.99, 'delivered', 'card'),
    (2, 849.98, 'shipped', 'paypal'),
    (3, 49.99, 'pending', 'card'),
    (1, 89.99, 'delivered', 'card');

INSERT INTO sales.order_items (order_id, product_name, quantity, unit_price) VALUES
    (1, 'Laptop Pro 16"', 1, 1499.99),
    (2, 'Smartphone X2', 1, 799.99),
    (2, 'Enceinte Bluetooth', 1, 49.99),
    (3, 'Python pour la Data Science', 1, 49.99),
    (4, 'Enceinte Bluetooth', 1, 89.99);

CREATE PUBLICATION cdc_pub FOR TABLE sales.customers, sales.orders, sales.order_items, sales.inventory;
