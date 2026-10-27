# Documento de decisiones técnicas

## Pipeline ETL con Apache Airflow y Data Warehouse — DataMart S.A.S.

## 1. Propósito

Este documento registra las decisiones técnicas aplicadas al pipeline construido para la prueba de desempeño. Las decisiones se explican a partir del flujo implementado y de los resultados obtenidos con el archivo `data.csv`.

## 2. Arquitectura de ejecución

Se utilizó Docker Compose para ejecutar los componentes del entorno de forma coordinada. Apache Airflow se fijó en la versión 3.1.5.

Se separó PostgreSQL en dos responsabilidades:

1. **Base de metadatos de Airflow:** mantiene el estado interno del orquestador.
2. **Data Warehouse PostgreSQL (`datamart`):** recibe los datos que consumen las consultas de análisis.

La separación evita mezclar el almacenamiento interno de la herramienta de orquestación con los datos del negocio y permite administrar ambos componentes de manera independiente.

El entorno incluye API server, scheduler, DAG processor y triggerer de Airflow. La interfaz se publica en `http://localhost:8080`, mientras que el puerto local `55432` se utiliza para acceder al PostgreSQL analítico.

## 3. Diseño del DAG

El DAG `etl_datamart` organiza el proceso mediante tres tareas encadenadas:

```text
extract_data >> transform_data >> load_data
```

- **`extract_data`:** lee la fuente de entrada y guarda el resultado extraído en `data/airflow_staging/extracted_sales.pkl`.
- **`transform_data`:** limpia y transforma los datos, generando `transformed_sales.pkl` y `daily_product_revenue.pkl`.
- **`load_data`:** carga el resultado transformado en las tablas analíticas de PostgreSQL.

El DAG tiene programación diaria, `catchup=False`, `max_active_runs=1` y reintentos configurados. `max_active_runs=1` evita que dos ejecuciones del mismo DAG se desarrollen simultáneamente.

## 4. Configuración y secretos

La conexión destino se administra mediante la Airflow Connection `warehouse_postgres`, en lugar de construir la conexión a la base de datos directamente dentro de la lógica de transformación.

Los parámetros operativos se administran mediante Variables de Airflow:

| Variable                | Decisión de uso                                                                    |
| ----------------------- | ----------------------------------------------------------------------------------- |
| `etl_input_path`      | Permite definir la ruta de la fuente de entrada desde la configuración de Airflow. |
| `etl_batch_size`      | Permite configurar el tamaño de lote del proceso.                                  |
| `etl_source_timezone` | Declara la zona horaria de referencia configurada para la fuente.                   |

Los valores utilizados en el entorno de práctica son `/opt/airflow/project/data/data.csv`, `5000` y `UTC`, respectivamente. La configuración sensible se mantiene en `.env`, separado del código fuente, y el repositorio incluye `.env.example` como referencia de configuración.

## 5. Fuente y alcance de los datos procesados

La ejecución documentada utiliza `data/data.csv`, un conjunto de transacciones de comercio electrónico con 541.909 filas y 8 columnas.

El pipeline procesa esta fuente para demostrar el recorrido ETL hasta PostgreSQL. La decisión de mantener una fuente de entrada definida y parametrizada permite ejecutar el flujo de manera reproducible en el entorno de práctica.

## 6. Decisiones de calidad de datos

### 6.1 Duplicados

La transformación elimina registros duplicados antes de publicar el resultado. Para la ejecución documentada, el conjunto pasó de 541.909 filas originales a 536.641 registros después de deduplicar.

Esta decisión reduce el riesgo de contar una misma fila repetida más de una vez en los resultados analíticos.

### 6.2 Precio unitario no válido

Se aplicó la regla de negocio que considera inválido un registro cuyo precio unitario sea menor o igual a cero. La ejecución identificó 2.512 registros bajo esa condición.

Los registros procesados conservan un indicador de validez (`is_valid`), y las agregaciones de negocio se calculan usando los registros válidos. De esta forma, los registros identificados como inválidos no contribuyen a las métricas válidas de ventas.

### 6.3 Clientes sin identificador

Los registros sin `CustomerID` se conservan en el conjunto procesado y se distinguen mediante `customer_missing`. Esta decisión mantiene la información de las transacciones aunque no sea posible atribuirlas a un cliente identificado. También permite comparar, mediante SQL, los registros asociados a clientes identificados frente a los que carecen de identificador.

### 6.4 Ventas, devoluciones y ajustes

Se mantienen métricas separadas para el importe bruto, los ajustes por devoluciones y el importe neto. Esto permite analizar el efecto de los ajustes en el resultado económico, en lugar de considerar todos los importes como ventas positivas.

En la ejecución documentada, el revenue bruto fue 10.642.110,80, los ajustes/devoluciones fueron -893.979,73 y el revenue neto resultante fue 9.748.131,07.

### 6.5 Fechas y zona horaria

La zona horaria de referencia del proceso se configura mediante `etl_source_timezone`, cuyo valor en el entorno de práctica es `UTC`. La estandarización temporal se gestiona como parte de las reglas de transformación del pipeline.

## 7. Modelo del repositorio analítico

El modelo de destino utiliza dos tablas de resultados principales:

### `public.sales_records`

Contiene los registros transaccionales procesados y los campos utilizados para análisis de ventas, ajustes, validez y presencia del identificador del cliente. La tabla conserva 536.641 registros en la ejecución documentada.

Mantener el detalle transaccional permite agrupar las operaciones posteriormente por fecha, producto, país o condición de identificación del cliente.

### `public.daily_product_revenue`

Contiene el resultado agregado por día y producto. La tabla cuenta con 278.011 filas en la ejecución documentada.

La agregación reduce el volumen necesario para consultas orientadas a tendencias temporales y desempeño por producto.

Se eligió conservar tanto el detalle como la agregación porque cada estructura sirve a un uso distinto: auditoría y análisis flexible en el detalle, y consultas analíticas más compactas en el agregado diario.

## 8. Idempotencia de la carga

La carga reemplaza el contenido de las tablas destino dentro de una operación transaccional antes de insertar el resultado procesado. Esta estrategia evita acumular las mismas filas en cada ejecución y permite reconstruir las tablas desde los archivos intermedios generados por el pipeline.

La ejecución manual del DAG `etl_datamart` registrada el 27 de octubre de 2026 finalizó con estado `success`. La comprobación de conteos en el warehouse mostró 536.641 registros en `public.sales_records` y 278.011 filas en `public.daily_product_revenue`.

La estrategia de reemplazo transaccional, junto con una entrada y transformación deterministas, está diseñada para que volver a cargar los mismos datos reproduzca el mismo estado de las tablas, en lugar de anexar duplicados.

## 9. Resultados cuantitativos de la ejecución

| Indicador                                   |     Resultado |
| ------------------------------------------- | ------------: |
| Filas de entrada                            |       541.909 |
| Filas después de deduplicar                |       536.641 |
| Registros válidos                          |       534.129 |
| Registros inválidos por `UnitPrice <= 0` |         2.512 |
| Filas en `public.sales_records`           |       536.641 |
| Filas en `public.daily_product_revenue`   |       278.011 |
| Revenue bruto                               | 10.642.110,80 |
| Ajustes/devoluciones                        |   -893.979,73 |
| Revenue neto                                |  9.748.131,07 |

La relación entre los importes reportados es:

```text
Revenue neto = Revenue bruto + ajustes/devoluciones
9.748.131,07 = 10.642.110,80 + (-893.979,73)
```

## 10. Consultas de análisis

Las consultas de negocio se agruparon en un solo archivo:

```text
sql/queries/consultas_negocio.sql
```

El archivo organiza las consultas mediante comentarios por pregunta: evolución mensual de ventas netas, análisis por producto, transacciones y ticket promedio por país, clientes identificados y sin identificar, descripciones inconsistentes, códigos de producto únicos, recomendación basada en resultados y control general de calidad.

## 11. Criterios de diseño

Las decisiones principales priorizan:

- Separar la orquestación de Airflow del almacenamiento analítico.
- Mantener las etapas ETL diferenciadas y con dependencias explícitas.
- Parametrizar la ruta de entrada y otros valores operativos mediante Variables.
- Centralizar la conexión a PostgreSQL mediante una Airflow Connection.
- Conservar indicadores de calidad para distinguir registros válidos e inválidos.
- Mantener detalle transaccional y una tabla agregada diaria para distintos tipos de consulta.
- Reemplazar los datos destino de manera transaccional para evitar duplicación acumulativa al repetir la carga.
- Registrar los resultados cuantitativos de la ejecución para facilitar su comprobación.

## 12. Resultado

El pipeline procesa la fuente `data.csv`, ejecuta las etapas de extracción, transformación y carga mediante Airflow, y publica resultados en PostgreSQL. La ejecución registrada terminó correctamente y dejó las tablas `public.sales_records` y `public.daily_product_revenue` con 536.641 y 278.011 filas, respectivamente.
