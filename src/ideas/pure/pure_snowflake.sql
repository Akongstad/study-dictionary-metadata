""" Used as an example. Promted from ChatGPT.
"""
-- SQL to create 1000 users in Snowflake
DECLARE 
  i INT DEFAULT 1;
  username STRING;
BEGIN
  WHILE i <= 1000 DO
    LET username = 'user_' || TO_CHAR(i, '000');
    EXECUTE IMMEDIATE 'CREATE USER IDENTIFIER(:username) PASSWORD = \'Password123!\'';
    LET i = i + 1;
  END WHILE;
END;