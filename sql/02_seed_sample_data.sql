-- ============================================================================
-- SEED SAMPLE DATA
-- ============================================================================

-- CUSTOMERS
INSERT INTO app.customers(full_name, email, phone_number)
VALUES
('Alice Nguyen',  'alice@example.com', '0123456789'),
('Bob Tran',      'bob@example.com',   '0987654321'),
('Charlie Pham',  'charlie@example.com','0901234567'),
('David Vu',      'david@example.com',  '0932211222'),
('Emma Le',       'emma@example.com',   '0912345678');

-- PRODUCTS
INSERT INTO app.products(product_name, category, price)
VALUES
('iPhone 15',            'Electronics', 23000000),
('Samsung Galaxy S24',   'Electronics', 21000000),
('MacBook Air M3',       'Laptop',      32000000),
('AirPods Pro 2',        'Audio',       5500000),
('Sony WH-1000XM5',      'Audio',       9000000),
('Logitech MX Master 3', 'Accessory',   2500000);

-- SAMPLE ORDERS
INSERT INTO app.orders(customer_id, order_date, status)
VALUES
(1, CURRENT_DATE - INTERVAL '5 days', 'completed'),
(2, CURRENT_DATE - INTERVAL '2 days', 'completed'),
(3, CURRENT_DATE - INTERVAL '10 days', 'completed'),
(1, CURRENT_DATE - INTERVAL '1 days', 'created'),
(5, CURRENT_DATE, 'created');

-- ORDER ITEMS
INSERT INTO app.order_items(order_id, product_id, quantity, price_each, subtotal)
VALUES
(1, 1, 1, 23000000, 23000000),
(1, 4, 1, 5500000,  5500000),

(2, 3, 1, 32000000, 32000000),

(3, 2, 1, 21000000, 21000000),
(3, 6, 2, 2500000,  5000000),

(4, 5, 1, 9000000,  9000000),

(5, 6, 1, 2500000,  2500000);
