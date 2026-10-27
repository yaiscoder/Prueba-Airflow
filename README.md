
# Pipeline ETL con Apache Airflow y Data Warehouse

## Caso DataMart S.A.S.

Pipeline de integración de datos que procesa transacciones de comercio electrónico, aplica reglas de calidad y publica información en un repositorio analítico PostgreSQL. La ejecución está orquestada con Apache Airflow dentro de Docker.

## 1. Objetivo

Automatizar las etapas de extracción, transformación y carga (ETL) de los datos de ventas para disponer de información consistente que permita analizar ventas brutas, devoluciones/ajustes, ventas netas y rendimiento por producto, periodo y país.

## 2. Arquitectura

El entorno utiliza servicios separados para la orquestación y para los datos analíticos:

- **Apache Airflow 3.1.5**: administra la ejecución del flujo.
- **API server de Airflow**: expone la interfaz web en `http://localhost:8080`.
- **Scheduler, DAG processor y triggerer**: componentes de ejecución de Airflow.
- **PostgreSQL de metadatos**: almacena el estado interno de Airflow.
- **PostgreSQL del Data Warehouse**: almacena las tablas analíticas.
- **Docker Compose**: coordina los servicios y los volúmenes compartidos.

La base de datos de metadatos de Airflow es independiente de la base `datamart`, utilizada como destino analítico. El servicio PostgreSQL del warehouse se publica en el puerto local `55432`.

### Flujo ETL

```text
data/data.csv
     |
     v
extract_data
     |
     v
data/airflow_staging/extracted_sales.pkl
     |
     v
transform_data
     |
     +--> data/airflow_staging/transformed_sales.pkl
     |
     +--> data/airflow_staging/daily_product_revenue.pkl
     |
     v
load_data
     |
     +--> PostgreSQL: public.sales_records
     |
     +--> PostgreSQL: public.daily_product_revenue
```

## 3. Tecnologías

- Docker y Docker Compose
- Apache Airflow 3.1.5
- PostgreSQL
- Python y pandas
- Airflow Connection para la conexión al warehouse
- Airflow Variables para configurar parámetros operativos

## 4. Fuente de datos procesada

El flujo procesa el archivo `data/data.csv`, correspondiente al conjunto de datos de transacciones de comercio electrónico utilizado en la prueba.

El archivo original contiene **541.909 filas** y 8 columnas. Los archivos intermedios se guardan en `data/airflow_staging/` para separar los resultados de extracción y transformación de la carga final.

## 5. Reglas de procesamiento aplicadas

- Se eliminan registros duplicados durante la transformación.
- Se evalúa la validez del precio unitario; los registros con `UnitPrice <= 0` se clasifican como inválidos.
- Los registros se conservan identificados mediante el indicador de validez, mientras que las agregaciones de negocio se calculan sobre los registros válidos.
- Las transacciones con `CustomerID` ausente se conservan y se identifican con el indicador `customer_missing`.
- Se calculan los importes brutos, los ajustes por devoluciones y el importe neto.
- Se genera una agregación diaria por producto para apoyar las consultas analíticas.

## 6. Configuración

### Requisitos

- Docker Desktop instalado y en ejecución.
- Git, si se clona el proyecto desde un repositorio.
- Archivo `.env` configurado a partir de `.env.example`.

No se deben subir al repositorio contraseñas ni valores secretos del archivo `.env`.

### Preparar el entorno

Desde la carpeta raíz del proyecto, donde está `docker-compose.yml`:

```powershell
Copy-Item .env.example .env
```

Edita `.env` con los valores locales correspondientes y levanta los servicios:

```powershell
docker compose up -d
```

Consulta el estado:

```powershell
docker compose ps
```

Abre la interfaz de Airflow:

```text
http://localhost:8080
```

Las credenciales de acceso son las definidas en las variables de entorno del proyecto.

## 7. Airflow Connection y Variables

El DAG utiliza la Connection `warehouse_postgres` para conectarse al PostgreSQL analítico.

Las Variables utilizadas son:

| Variable                | Uso                                                        |
| ----------------------- | ---------------------------------------------------------- |
| `etl_input_path`      | Ruta del archivo de entrada dentro del entorno de Airflow. |
| `etl_batch_size`      | Parámetro de tamaño de lote para el procesamiento/carga. |
| `etl_source_timezone` | Zona horaria de referencia de la fuente.                   |

Valores usados en el entorno de práctica:

```text
etl_input_path=/opt/airflow/project/data/data.csv
etl_batch_size=5000
etl_source_timezone=UTC
```

Para comprobar la configuración desde la interfaz, abre **Admin → Connections** y busca `warehouse_postgres`; después abre **Admin → Variables** y consulta las variables anteriores.

## 8. Ejecutar el pipeline

1. Confirma que los servicios están en ejecución con `docker compose ps`.
2. Abre `http://localhost:8080`.
3. Busca el DAG `etl_datamart`.
4. Actívalo si está pausado.
5. Ejecuta el DAG desde la interfaz y revisa el estado de `extract_data`, `transform_data` y `load_data`.

El DAG está organizado en tres tareas con dependencias explícitas:

```text
extract_data >> transform_data >> load_data
```

La programación del DAG es diaria, con `catchup=False`, `max_active_runs=1` y reintentos configurados para los fallos de tareas.

## 9. Verificar la carga en PostgreSQL

Desde PowerShell, en la raíz del proyecto:

```powershell
docker compose exec postgres psql -U datamart_user -d datamart
```

En el cliente `psql`, consulta el volumen cargado:

```sql
SELECT COUNT(*) AS total_registros
FROM public.sales_records;

SELECT COUNT(*) AS total_agregados
FROM public.daily_product_revenue;
```

Para salir de `psql`:

```text
\q
```

También puedes ejecutar las consultas directamente desde PowerShell:

```powershell
docker compose exec postgres psql -U datamart_user -d datamart -c "SELECT COUNT(*) FROM public.sales_records;"
docker compose exec postgres psql -U datamart_user -d datamart -c "SELECT COUNT(*) FROM public.daily_product_revenue;"
```

## 10. Resultados de la ejecución documentada

En la ejecución registrada para el archivo de entrada se obtuvieron estos resultados:

| Métrica                                                      |     Resultado |
| ------------------------------------------------------------- | ------------: |
| Filas originales extraídas                                   |       541.909 |
| Registros después de deduplicar                              |       536.641 |
| Registros válidos                                            |       534.129 |
| Registros inválidos por precio unitario menor o igual a cero |         2.512 |
| Registros en `public.sales_records`                         |       536.641 |
| Filas en `public.daily_product_revenue`                     |       278.011 |
| Revenue bruto                                                 | 10.642.110,80 |
| Ajustes/devoluciones                                          |   -893.979,73 |
| Revenue neto                                                  |  9.748.131,07 |

Los valores monetarios corresponden al resultado de la transformación aplicada al conjunto de datos procesado.

## 11. Consultas de negocio

Las consultas SQL de análisis están centralizadas en:

```text
sql/queries/consultas_negocio.sql
```

El archivo reúne consultas para la evolución mensual de ventas netas, análisis por producto, transacciones por país, comparación entre clientes identificados y sin identificar, consistencia de descripciones, códigos de producto únicos y control general de calidad.

Para ejecutarlo:

```powershell
Get-Content .\sql\queries\consultas_negocio.sql -Raw |
docker compose exec -T postgres psql -U datamart_user -d datamart
```

## 12. Estructura relevante del proyecto

```text
.
├── dags/
│   └── etl_datamart.py
├── data/
│   ├── data.csv
│   └── airflow_staging/
│       ├── extracted_sales.pkl
│       ├── transformed_sales.pkl
│       └── daily_product_revenue.pkl
├── sql/
│   └── queries/
│       └── consultas_negocio.sql
├── .env.example
├── .gitignore
└── docker-compose.yml
```

Los archivos `.pkl` corresponden a resultados intermedios generados durante la ejecución. El archivo `.env` contiene configuración local y no debe publicarse.

## 13. Detener el entorno

```powershell
docker compose down
```

Para detener y eliminar también los volúmenes persistentes:

```powershell
docker compose down -v
```

**Advertencia:** `docker compose down -v` elimina los volúmenes del entorno, incluidos los datos persistidos en PostgreSQL.
