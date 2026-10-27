-- 1. EVOLUCION MENSUAL DE LAS VENTAS NETAS DESPUES DE DEVOLUCIONES

SELECT
    DATE_TRUNC('month', "InvoiceDate")::date AS mes,
    ROUND(SUM(gross_revenue)::numeric, 2) AS ventas_brutas,
    ROUND(SUM(return_adjustment_amount)::numeric, 2) AS ajustes_devoluciones,
    ROUND(SUM(net_revenue)::numeric, 2) AS ventas_netas
FROM public.sales_records
WHERE is_valid = TRUE
GROUP BY DATE_TRUNC('month', "InvoiceDate")::date
ORDER BY mes;


-- 2.1. TOP 10 PRODUCTOS POR INGRESOS NETOS

SELECT
    "StockCode" AS codigo_producto,
    MAX("Description") AS descripcion,
    ROUND(SUM(gross_revenue)::numeric, 2) AS ventas_brutas,
    ROUND(SUM(return_adjustment_amount)::numeric, 2)
        AS ajustes_devoluciones,
    ROUND(SUM(net_revenue)::numeric, 2) AS ventas_netas
FROM public.sales_records
WHERE is_valid = TRUE
GROUP BY "StockCode"
ORDER BY ventas_netas DESC
LIMIT 10;

-- 2.2. TOP 10 PRODUCTOS POR TASA DE DEVOLUCION

SELECT
    "StockCode" AS codigo_producto,
    MAX("Description") AS descripcion,
    ROUND(SUM(gross_revenue)::numeric, 2) AS ventas_brutas,
    ROUND(ABS(SUM(return_adjustment_amount))::numeric, 2)
        AS valor_devoluciones,
    ROUND(
        (
            100.0::numeric
            * ABS(SUM(return_adjustment_amount))::numeric
            / NULLIF(SUM(gross_revenue)::numeric, 0)
        ),
        2
    ) AS tasa_devolucion_pct
FROM public.sales_records
WHERE is_valid = TRUE
GROUP BY "StockCode"
HAVING SUM(gross_revenue) > 0
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;



-- 3.1. PAISES CON MAYOR NUMERO DE TRANSACCIONES


SELECT
    "Country" AS pais,
    COUNT(DISTINCT "InvoiceNo") AS total_transacciones,
    COUNT(*) AS total_registros,
    ROUND(SUM(net_revenue)::numeric, 2) AS ventas_netas
FROM public.sales_records
WHERE is_valid = TRUE
GROUP BY "Country"
ORDER BY total_transacciones DESC;


-- 3.2. VARIACION MENSUAL DEL TICKET PROMEDIO POR PAIS


WITH ventas_mensuales AS (
    SELECT
        "Country" AS pais,
        DATE_TRUNC('month', "InvoiceDate")::date AS mes,
        "InvoiceNo" AS factura,
        SUM(net_revenue) AS valor_factura
    FROM public.sales_records
    WHERE is_valid = TRUE
    GROUP BY
        "Country",
        DATE_TRUNC('month', "InvoiceDate")::date,
        "InvoiceNo"
),
tickets AS (
    SELECT
        pais,
        mes,
        COUNT(*) AS total_transacciones,
        AVG(valor_factura) AS ticket_promedio
    FROM ventas_mensuales
    GROUP BY pais, mes
)
SELECT
    pais,
    mes,
    total_transacciones,
    ROUND(ticket_promedio::numeric, 2) AS ticket_promedio,
    ROUND(
        (
            100.0 * (
                ticket_promedio
                - LAG(ticket_promedio) OVER (
                    PARTITION BY pais ORDER BY mes
                )
            )
            / NULLIF(
                LAG(ticket_promedio) OVER (
                    PARTITION BY pais ORDER BY mes
                ),
                0
            )
        )::numeric,
        2
    ) AS variacion_mensual_pct
FROM tickets
ORDER BY pais, mes;



-- 4. CLIENTES IDENTIFICADOS VS. CLIENTES SIN IDENTIFICAR

SELECT
    CASE
        WHEN customer_missing = TRUE THEN 'Sin identificar'
        ELSE 'Identificado'
    END AS tipo_cliente,
    COUNT(*) AS cantidad_registros,
    COUNT(DISTINCT "InvoiceNo") AS transacciones,
    ROUND(SUM(net_revenue)::numeric, 2) AS ventas_netas,
    ROUND(AVG(net_revenue)::numeric, 2) AS ingreso_neto_promedio
FROM public.sales_records
WHERE is_valid = TRUE
GROUP BY
    CASE
        WHEN customer_missing = TRUE THEN 'Sin identificar'
        ELSE 'Identificado'
    END
ORDER BY tipo_cliente;


-- 5.1. DESCRIPCIONES INCONSISTENTES PARA UN MISMO CODIGO


SELECT
    "StockCode" AS codigo_producto,
    COUNT(DISTINCT UPPER(TRIM("Description")))
        AS cantidad_descripciones,
    ARRAY_AGG(
        DISTINCT UPPER(TRIM("Description"))
        ORDER BY UPPER(TRIM("Description"))
    ) AS descripciones_encontradas
FROM public.sales_records
WHERE "StockCode" IS NOT NULL
  AND "Description" IS NOT NULL
  AND TRIM("Description") <> ''
GROUP BY "StockCode"
HAVING COUNT(DISTINCT UPPER(TRIM("Description"))) > 1
ORDER BY cantidad_descripciones DESC, codigo_producto;


-- 5.2. TOTAL DE CODIGOS DE PRODUCTO UNICOS


SELECT
    COUNT(DISTINCT "StockCode") AS total_codigos_unicos
FROM public.sales_records
WHERE "StockCode" IS NOT NULL;



-- 6. INFORMACION PARA RECOMENDAR UN PRODUCTO AL EQUIPO

SELECT
    "StockCode" AS codigo_producto,
    MAX("Description") AS descripcion,
    COUNT(DISTINCT "InvoiceNo") AS transacciones,
    ROUND(SUM(gross_revenue)::numeric, 2) AS ventas_brutas,
    ROUND(
        ABS(SUM(return_adjustment_amount))::numeric,
        2
    ) AS valor_devoluciones,
    ROUND(SUM(net_revenue)::numeric, 2) AS ventas_netas,
    ROUND(
        (
            100.0::numeric
            * ABS(SUM(return_adjustment_amount))::numeric
            / NULLIF(SUM(gross_revenue)::numeric, 0)
        ),
        2
    ) AS tasa_devolucion_pct
FROM public.sales_records
WHERE is_valid = TRUE
GROUP BY "StockCode"
HAVING SUM(gross_revenue) > 0
ORDER BY ventas_netas DESC
LIMIT 20;



-- 7. CONTROL GENERAL DE CALIDAD DE LOS DATOS


SELECT
    COUNT(*) AS total_registros,
    COUNT(*) FILTER (WHERE is_valid = TRUE) AS registros_validos,
    COUNT(*) FILTER (WHERE is_valid = FALSE) AS registros_invalidos,
    COUNT(*) FILTER (
        WHERE customer_missing = TRUE
    ) AS registros_sin_cliente,
    COUNT(DISTINCT "StockCode") AS productos_unicos,
    COUNT(DISTINCT "InvoiceNo") AS transacciones_unicas
FROM public.sales_records;
