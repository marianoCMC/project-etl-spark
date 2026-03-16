-- Esquema inicial para el dataset Olist
-- Este script se ejecuta automáticamente al levantar el contenedor de PostgreSQL

CREATE SCHEMA IF NOT EXISTS olist;

-- Tabla de pedidos
CREATE TABLE IF NOT EXISTS olist.orders (
    order_id              VARCHAR(32) PRIMARY KEY,
    customer_id           VARCHAR(32),
    order_status          VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at     TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- Tabla de clientes
CREATE TABLE IF NOT EXISTS olist.customers (
    customer_id           VARCHAR(32) PRIMARY KEY,
    customer_unique_id    VARCHAR(32),
    customer_zip_code_prefix VARCHAR(10),
    customer_city         VARCHAR(100),
    customer_state        VARCHAR(2)
);

-- Tabla de productos
CREATE TABLE IF NOT EXISTS olist.products (
    product_id            VARCHAR(32) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_weight_g      FLOAT,
    product_length_cm     FLOAT,
    product_height_cm     FLOAT,
    product_width_cm      FLOAT
);

-- Tabla de items por pedido
CREATE TABLE IF NOT EXISTS olist.order_items (
    order_id              VARCHAR(32),
    order_item_id         INT,
    product_id            VARCHAR(32),
    seller_id             VARCHAR(32),
    shipping_limit_date   TIMESTAMP,
    price                 FLOAT,
    freight_value         FLOAT,
    PRIMARY KEY (order_id, order_item_id)
);

-- Tabla de pagos
CREATE TABLE IF NOT EXISTS olist.order_payments (
    order_id              VARCHAR(32),
    payment_sequential    INT,
    payment_type          VARCHAR(30),
    payment_installments  INT,
    payment_value         FLOAT,
    PRIMARY KEY (order_id, payment_sequential)
);

-- Tabla de reviews
CREATE TABLE IF NOT EXISTS olist.order_reviews (
    review_id             VARCHAR(32),
    order_id              VARCHAR(32),
    review_score          INT,
    review_creation_date  TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);
