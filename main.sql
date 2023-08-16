CREATE STREAM stock_dato (symbol VARCHAR, price DOUBLE, volume INT, timestamp VARCHAR)
  WITH (kafka_topic='stock-pedido', value_format='json', partitions=1);

-- Minimo precio ponderado por simbolo de moneda
CREATE TABLE Precio_Ponderado_Simbolo AS
    SELECT symbol,
           avg(price/volume) AS avg_pond
    FROM stock_dato
    GROUP BY symbol
    EMIT CHANGES;

-- Transacciones procesadas por simbolo
CREATE TABLE Transacciones_x_Simbolo AS
    SELECT symbol,
           count(symbol) AS transac
    FROM stock_dato
    GROUP BY symbol
    EMIT CHANGES;

-- Maximo Precio por Simbolo
CREATE TABLE Maximo_Precio_x_Simbolo AS
    SELECT symbol,
           max(price) AS max_price
    FROM stock_dato
    GROUP BY symbol
    EMIT CHANGES;

-- Minimo Precio por Simbolo
CREATE TABLE Minimo_Precio_x_Simbolo AS
    SELECT symbol,
           min(price) AS min_price
    FROM stock_dato
    GROUP BY symbol
    EMIT CHANGES;

-- Analisis Precio por Simbolo
CREATE TABLE Analisis_Precio_x_Simbolo AS
    SELECT symbol,
           sum(price) AS sum_price,
           sum(volume) AS sum_volume,
           avg(price/volume) AS avg_pond,
           count(symbol) AS transac,
           max(price) AS max_price,
           min(price) AS min_price
    FROM stock_dato
    GROUP BY symbol
     EMIT CHANGES;

--Pregunta 1
SELECT * FROM Precio_Ponderado_Simbolo;
--Pregunta 2
SELECT * FROM Transacciones_x_Simbolo;
--Pregunta 3
SELECT * FROM Maximo_Precio_x_Simbolo;
--Pregunta 4
SELECT * FROM Minimo_Precio_x_Simbolo;

--Analisis global de las 4 preguntas
SELECT * FROM Analisis_Precio_x_Simbolo;
