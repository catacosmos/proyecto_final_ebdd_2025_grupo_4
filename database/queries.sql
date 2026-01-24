
-- CONSULTAS

-- 1. Escultura más cara vendida por galería en el último mes
SELECT 
    g.nombre AS nombre_galeria,
    p.nombre AS obra,
    MAX(v.monto) AS precio_maximo
FROM VENTA v
JOIN PRODUCTO p ON v.id_prod = p.id_prod
JOIN TIPO_ARTE ta ON p.id_tipo = ta.id_tipo
JOIN LOCAL l ON p.id_local = l.id_local
JOIN GALERIA g ON l.id_galeria = g.id_galeria
WHERE ta.descripcion = 'Escultura'
  AND date(v.fecha) >= date('now', 'start of month', '-1 month')
GROUP BY g.id_galeria, g.nombre;

-- 2. Pintura más económica vendida el año pasado
SELECT 
    p.nombre AS nombre_pintura,
    v.monto AS precio,
    g.nombre AS galeria
FROM VENTA v
JOIN PRODUCTO p ON v.id_prod = p.id_prod
JOIN TIPO_ARTE ta ON p.id_tipo = ta.id_tipo
JOIN LOCAL l ON p.id_local = l.id_local
JOIN GALERIA g ON l.id_galeria = g.id_galeria
WHERE ta.descripcion = 'Pintura'
  AND strftime('%Y', v.fecha) = strftime('%Y', date('now', '-1 year'))
ORDER BY v.monto ASC
LIMIT 1;

-- 3. Vendedor con más ventas por galería
WITH RankingVendedores AS (
    SELECT 
        g.nombre AS nombre_galeria,
        e.nombre AS nombre_vendedor,
        COUNT(v.id_venta) AS cantidad_ventas,
        RANK() OVER (PARTITION BY g.id_galeria ORDER BY COUNT(v.id_venta) DESC) as ranking
    FROM VENTA v
    JOIN EMPLEADO e ON v.id_emp = e.id_emp
    JOIN GALERIA g ON e.id_galeria = g.id_galeria
    GROUP BY g.id_galeria, g.nombre, e.id_emp, e.nombre
)
SELECT nombre_galeria, nombre_vendedor, cantidad_ventas
FROM RankingVendedores
WHERE ranking = 1;

-- 4. Cliente que ha invertido más en arte, por año
WITH InversionClientes AS (
    SELECT 
        strftime('%Y', v.fecha) AS anio,
        c.nombre AS nombre_cliente,
        SUM(v.monto) AS total_invertido,
        RANK() OVER (PARTITION BY strftime('%Y', v.fecha) ORDER BY SUM(v.monto) DESC) as ranking
    FROM VENTA v
    JOIN CLIENTE c ON v.id_cli = c.id_cli
    GROUP BY anio, c.id_cli, c.nombre
)
SELECT anio, nombre_cliente, total_invertido
FROM InversionClientes
WHERE ranking = 1
ORDER BY anio DESC;

-- 5. Tipo de arte más vendido por año
WITH VentasPorTipo AS (
    SELECT 
        strftime('%Y', v.fecha) AS anio,
        ta.descripcion AS tipo_arte,
        COUNT(v.id_venta) AS cantidad_vendida,
        RANK() OVER (PARTITION BY strftime('%Y', v.fecha) ORDER BY COUNT(v.id_venta) DESC) as ranking
    FROM VENTA v
    JOIN PRODUCTO p ON v.id_prod = p.id_prod
    JOIN TIPO_ARTE ta ON p.id_tipo = ta.id_tipo
    GROUP BY anio, ta.id_tipo, ta.descripcion
)
SELECT anio, tipo_arte, cantidad_vendida
FROM VentasPorTipo
WHERE ranking = 1
ORDER BY anio DESC;

-- 6. Lista de regiones, y sus galerías por comuna
SELECT 
    r.nombre AS region,
    c.nombre AS comuna,
    g.nombre AS galeria
FROM REGION r
JOIN COMUNA c ON c.id_reg = r.id_reg
JOIN GALERIA g ON g.id_com = c.id_com
ORDER BY r.nombre, c.nombre, g.nombre;

-- 7. Venta más cara por mes por galería en el 2019
WITH Ventas2019 AS (
    SELECT 
        g.nombre AS galeria,
        strftime('%m', v.fecha) AS mes,
        v.monto,
        RANK() OVER (PARTITION BY g.id_galeria, strftime('%m', v.fecha) ORDER BY v.monto DESC) as ranking
    FROM VENTA v
    JOIN PRODUCTO p ON v.id_prod = p.id_prod
    JOIN LOCAL l ON p.id_local = l.id_local
    JOIN GALERIA g ON l.id_galeria = g.id_galeria
    WHERE strftime('%Y', v.fecha) = '2019'
)
SELECT galeria, mes, monto AS venta_maxima
FROM Ventas2019
WHERE ranking = 1
ORDER BY galeria, mes;

-- 8. Cliente que ha comprado más pinturas entre el 2019 y 2021
SELECT 
    c.nombre AS nombre_cliente,
    COUNT(v.id_venta) AS cantidad_pinturas
FROM VENTA v
JOIN CLIENTE c ON v.id_cli = c.id_cli
JOIN PRODUCTO p ON v.id_prod = p.id_prod
JOIN TIPO_ARTE ta ON p.id_tipo = ta.id_tipo
WHERE ta.descripcion = 'Pintura'
  AND strftime('%Y', v.fecha) BETWEEN '2019' AND '2021'
GROUP BY c.id_cli, c.nombre
ORDER BY cantidad_pinturas DESC
LIMIT 1;

-- 9. Promedio de sueldos de hombres y mujeres mensual por galería
SELECT 
    g.nombre AS galeria,
    e.genero,
    COUNT(e.id_emp) AS cantidad_empleados,
    AVG(e.sueldo) AS sueldo_promedio
FROM EMPLEADO e
JOIN GALERIA g ON e.id_galeria = g.id_galeria
GROUP BY g.id_galeria, g.nombre, e.genero
ORDER BY g.nombre, e.genero;

-- 10. Venta más pesada en kilogramos por mes en los últimos 12 meses
WITH VentasPesadas AS (
    SELECT 
        strftime('%Y', v.fecha) AS anio,
        strftime('%m', v.fecha) AS mes,
        p.nombre AS obra,
        p.peso,
        RANK() OVER (PARTITION BY strftime('%Y', v.fecha), strftime('%m', v.fecha) ORDER BY p.peso DESC) as ranking
    FROM VENTA v
    JOIN PRODUCTO p ON v.id_prod = p.id_prod
    WHERE date(v.fecha) >= date('now', '-12 months')
)
SELECT anio, mes, obra, peso
FROM VentasPesadas
WHERE ranking = 1
ORDER BY anio DESC, mes DESC;