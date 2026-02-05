-- ============================================================================
-- CREATE OLTP SCHEMA FOR DATA ENGINEERING BOOTCAMP
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS app;

-- ============================================================================
-- CUSTOMERS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS app.customers (
    customer_id     SERIAL PRIMARY KEY,
    full_name       VARCHAR(255) NOT NULL,
    email           VARCHAR(255) UNIQUE,
    phone_number    VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- PRODUCTS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS app.products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(255) NOT NULL,
    category        VARCHAR(100),
    price           NUMERIC(10,2) NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- ORDERS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS app.orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INT NOT NULL REFERENCES app.customers(customer_id),
    order_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    total_amount    NUMERIC(12,2),
    status          VARCHAR(50) DEFAULT 'created',
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- ORDER ITEMS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS app.order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INT NOT NULL REFERENCES app.orders(order_id),
    product_id      INT NOT NULL REFERENCES app.products(product_id),
    quantity        INT NOT NULL CHECK (quantity > 0),
    price_each      NUMERIC(10,2) NOT NULL,
    subtotal        NUMERIC(12,2),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- UPDATE TRIGGER: AUTO-CALCULATE ORDER TOTAL
-- ============================================================================
CREATE OR REPLACE FUNCTION app.update_order_total()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE app.orders
    SET total_amount = (
        SELECT SUM(subtotal)
        FROM app.order_items
        WHERE order_id = NEW.order_id
    )
    WHERE order_id = NEW.order_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_items_total
AFTER INSERT OR UPDATE ON app.order_items
FOR EACH ROW EXECUTE FUNCTION app.update_order_total();
