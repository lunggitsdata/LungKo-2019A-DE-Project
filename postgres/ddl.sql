CREATE TABLE transactions(
  __connect_topic CHAR(20) NOT NULL,
  __connect_partition INT NOT NULL,
  __connect_offset BIGINT NOT NULL,
  date CHAR(10) NOT NULL,
  shares INT NOT NULL,
  symbol CHAR(10) NOT NULL,
  price REAL NOT NULL,
  time CHAR(5) NOT NULL,
  PRIMARY KEY(__connect_topic, __connect_partition, __connect_offset)
);

CREATE TABLE portfolio (
  symbol CHAR(10) NOT NULL,
  price REAL NOT NULL,
  shares INT NOT NULL
);

CREATE INDEX portfolio_idx ON portfolio(symbol);

CREATE or REPLACE FUNCTION check_transaction() RETURNS TRIGGER AS $$
DECLARE
  owned_shares integer := 0;  /* number of owned shares */
BEGIN
  SELECT shares INTO owned_shares FROM portfolio WHERE symbol = NEW.symbol;
  IF NEW.shares > -1 THEN /* buy */
    IF owned_shares > 0 THEN
	  UPDATE portfolio SET shares = (owned_shares + NEW.shares) WHERE symbol = NEW.symbol; 
	ELSE
	  INSERT INTO portfolio (symbol, price, shares) VALUES (NEW.symbol, NEW.price, NEW.shares);
	END IF;
  ELSE  /* sell */
    IF owned_shares > 0 THEN
	  NEW.shares = -1*owned_shares;  /* empty shares fast */
	  DELETE FROM portfolio WHERE symbol = NEW.symbol;
	ELSE
	  RETURN NULL;
	END IF;
  END IF;
  RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trig_transactions
BEFORE INSERT ON transactions
FOR EACH ROW EXECUTE PROCEDURE check_transaction();
