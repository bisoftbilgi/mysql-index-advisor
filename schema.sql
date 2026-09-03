-- Demo schema for the tuning wizard.
-- Intentionally almost no secondary indexes, so EXPLAIN shows table scans

CREATE DATABASE IF NOT EXISTS tuning_demo
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE tuning_demo;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;

CREATE TABLE customers (
  customer_id INT NOT NULL AUTO_INCREMENT,
  email VARCHAR(120) NOT NULL,
  full_name VARCHAR(120) NOT NULL,
  city VARCHAR(80) NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (customer_id)
) ENGINE=InnoDB;


CREATE TABLE products (
  product_id INT NOT NULL AUTO_INCREMENT,
  sku VARCHAR(40) NOT NULL,
  product_name VARCHAR(120) NOT NULL,
  category VARCHAR(60) NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (product_id)
) ENGINE=InnoDB;

CREATE TABLE orders (
  order_id INT NOT NULL AUTO_INCREMENT,
  customer_id INT NOT NULL,
  status VARCHAR(20) NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  order_date DATE NOT NULL,
  PRIMARY KEY (order_id)
) ENGINE=InnoDB;

CREATE TABLE order_items (
  item_id INT NOT NULL AUTO_INCREMENT,
  order_id INT NOT NULL,
  product_id INT NOT NULL,
  qty INT NOT NULL,
  PRIMARY KEY (item_id)
) ENGINE=InnoDB;
