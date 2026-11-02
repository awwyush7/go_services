CREATE TABLE items (
  id          BIGINT       NOT NULL AUTO_INCREMENT,
  name        VARCHAR(255) NOT NULL,
  category    VARCHAR(100) NOT NULL,
  price_paise BIGINT       NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE inventory (
  item_id BIGINT NOT NULL,
  stock   BIGINT NOT NULL,
  PRIMARY KEY (item_id),
  CONSTRAINT chk_inventory_stock CHECK (stock >= 0)
);
