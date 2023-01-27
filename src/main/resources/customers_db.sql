

CREATE TABLE IF NOT EXISTS customers (
  id VARCHAR(32),
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(255)
);

INSERT INTO customers (id, first_name, last_name, email)
VALUES
("1", "Scott", "Haines", "scott@coffeeco.com"),
("2", "John", "Hamm", "john.hamm@acme.com"),
("3", "Milo", "Haines", "mhaines@coffeeco.com");


