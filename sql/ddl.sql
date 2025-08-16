USE analytics_portfolio;

-- =============== DIMENSIONES ===============

CREATE TABLE IF NOT EXISTS dim_channel (
  channel_id INT AUTO_INCREMENT PRIMARY KEY,
  channel_name VARCHAR(100) NOT NULL,
  channel_group VARCHAR(50) NULL,
  UNIQUE KEY uk_channel (channel_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id INT AUTO_INCREMENT PRIMARY KEY,
  external_id VARCHAR(64) UNIQUE,
  signup_date DATE,
  country VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============== HECHOS BASE ===============

CREATE TABLE IF NOT EXISTS fact_touches (
  touch_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_ts DATETIME NOT NULL,
  customer_id INT NULL,
  channel_id INT NOT NULL,
  campaign VARCHAR(100),
  session_id VARCHAR(64),
  revenue_at_event DECIMAL(12,2) DEFAULT 0,
  FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
  FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id),
  KEY k_event_ts (event_ts),
  KEY k_customer (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fact_orders (
  order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_ts DATETIME NOT NULL,
  customer_id INT NOT NULL,
  amount DECIMAL(12,2) NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
  KEY k_order_ts (order_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fact_marketing_spend (
  spend_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  spend_date DATE NOT NULL,
  channel_id INT NOT NULL,
  spend DECIMAL(12,2) NOT NULL,
  FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id),
  UNIQUE KEY uk_spend (spend_date, channel_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============== NUEVO: EVENTOS ===============

CREATE TABLE IF NOT EXISTS fact_events (
  event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_ts DATETIME NOT NULL,
  customer_id INT NOT NULL,
  channel_id INT NOT NULL,
  event_type VARCHAR(50) NOT NULL, -- view_product, add_to_cart, purchase
  product_id BIGINT NULL,
  order_id BIGINT NULL,
  FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
  FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id),
  FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
  KEY k_event_ts (event_ts),
  KEY k_event_type (event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============== AGREGADOS/DERIVADAS ===============

CREATE TABLE IF NOT EXISTS fact_attribution (
  attr_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  model VARCHAR(50) NOT NULL,
  channel_id INT NOT NULL,
  weight DECIMAL(9,6) NOT NULL,
  attributed_revenue DECIMAL(12,2) NOT NULL,
  FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
  FOREIGN KEY (channel_id) REFERENCES dim_channel(channel_id),
  KEY k_model (model)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fact_ltv_cohort (
  ltv_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  cohort_month DATE NOT NULL,
  customer_id INT NOT NULL,
  horizon_days INT NOT NULL,
  revenue DECIMAL(12,2) NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
  KEY k_cohort (cohort_month),
  UNIQUE KEY uk_ltv (cohort_month, customer_id, horizon_days)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;