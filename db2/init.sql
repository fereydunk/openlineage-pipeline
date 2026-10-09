-- Seed the ORDERS table in Db2. Run as db2inst1 (default schema DB2INST1).
-- The JDBC source connector polls on the auto-increment ID column (mode=incrementing).

DROP TABLE ORDERS;

CREATE TABLE ORDERS (
    ID          INTEGER       NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
    ORDER_ID    VARCHAR(20)   NOT NULL,
    CUSTOMER    VARCHAR(60)   NOT NULL,
    ITEM        VARCHAR(40)   NOT NULL,
    QUANTITY    INTEGER       NOT NULL,
    AMOUNT_USD  DECIMAL(10,2) NOT NULL,
    ORDER_TS    TIMESTAMP     NOT NULL DEFAULT CURRENT TIMESTAMP,
    PRIMARY KEY (ID)
);

INSERT INTO ORDERS (ORDER_ID, CUSTOMER, ITEM, QUANTITY, AMOUNT_USD) VALUES
    ('ORD-1001', 'Acme Corp',        'Widget',      12,  1440.00),
    ('ORD-1002', 'Globex',           'Gadget',       3,   299.97),
    ('ORD-1003', 'Initech',          'Sprocket',    50,  2500.00),
    ('ORD-1004', 'Umbrella LLC',     'Widget',       7,   840.00),
    ('ORD-1005', 'Soylent Inc',      'Cog',          2,    49.98),
    ('ORD-1006', 'Hooli',            'Gadget',      25,  2499.75),
    ('ORD-1007', 'Stark Industries', 'Reactor',      1,  9999.99),
    ('ORD-1008', 'Wayne Enterprises','Sprocket',    15,   750.00),
    ('ORD-1009', 'Cyberdyne',        'Cog',         40,   999.60),
    ('ORD-1010', 'Tyrell Corp',      'Widget',       9,  1080.00);
