import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

import requests
import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from scripts.masking_transform import load_masking_rules, apply_masking

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURACIÓN - Usar variables de entorno (NO hardcoding)
# =============================================================================
API_KEY = os.getenv('API_KEY')
API_BASE_URL = os.getenv('API_BASE_URL', 'http://host.docker.internal:5000/api')

DB_CONFIG = {
    'user': os.getenv('DB_USER', 'user_dbt'),
    'password': os.getenv('DB_PASS', 'password_dbt'),
    'host': os.getenv('DB_HOST', 'postgres_warehouse'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'db_warehouse')
}

# Conexión SQLAlchemy para operaciones directas
DB_URI = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# =============================================================================
# FUNCIÓN DE EXTRACCIÓN RESILIENTE (con retry para errores 429/500)
# =============================================================================
def fetch_with_retry(
    url: str,
    headers: Dict,
    params: Dict,
    max_retries: int = 5,
    base_delay: int = 2
) -> Optional[Dict]:
    """
    Realiza request a la API con backoff exponencial para manejar rate limits.

    Args:
        url: Endpoint de la API
        headers: Headers de la request (incluye API key)
        params: Parámetros de query (page, limit)
        max_retries: Número máximo de reintentos
        base_delay: Delay inicial en segundos para backoff

    Returns:
        Dict con la respuesta JSON o None si falla después de todos los intentos
    """
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

    @retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=base_delay, min=2, max=30),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True
    )
    def _request():
        response = requests.get(url, headers=headers, params=params, timeout=30)

        # Manejar específicamente errores de la API simulada
        if response.status_code == 429:
            logger.warning(f"⚠️ Rate limit (429) - Reintentando en {base_delay}s...")
            raise requests.exceptions.RequestException("Rate limit exceeded")
        elif response.status_code == 500:
            logger.warning(f"⚠️ Server error (500) - Reintentando en {base_delay}s...")
            raise requests.exceptions.RequestException("Internal server error")
        elif response.status_code == 401:
            logger.error("❌ Error de autenticación (401) - No reintentar")
            response.raise_for_status()  # Esto lanzará la excepción y detendrá el retry
        elif response.status_code != 200:
            logger.error(f"❌ Error inesperado {response.status_code}: {response.text}")
            response.raise_for_status()

        return response.json()

    try:
        return _request()
    except Exception as e:
        logger.error(f"❌ Fallo después de {max_retries} intentos: {e}")
        return None


# =============================================================================
# DEFINICIÓN DEL DAG CON TASKFLOW API
# =============================================================================
@dag(
    dag_id='dag_wap_unl_final',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',  # Ejecución diaria
    catchup=False,
    tags=['ETL', 'WAP', 'GreatExpectations', 'UNL'],
    description='Pipeline ETL con patrón WAP para datos de transacciones',
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: logger.error(f"DAG failed: {context}")
    }
)
def taller_etl_unl_wap():

    # -------------------------------------------------------------------------
    # FASE 1: WRITE - Extracción y carga a esquema AUDIT
    # -------------------------------------------------------------------------
    @task(task_id='write_to_audit', retries=3, retry_delay=timedelta(seconds=30))
    def write_to_audit() -> Dict[str, int]:
        """
        Extrae datos de la API y los carga en el esquema audit.raw_transactions.

        Returns:
            Dict con métricas de la extracción
        """
        logger.info("🚀 Iniciando fase WRITE: Extracción de API")

        pg_hook = PostgresHook(postgres_conn_id='postgres_warehouse_conn')

        # Limpiar tabla de auditoría para esta ejecución
        pg_hook.run("TRUNCATE TABLE audit.raw_transactions;")
        logger.info("🗑️ Tabla audit.raw_transactions limpiada")

        # Configurar request
        url = f"{API_BASE_URL}/transactions"
        headers = {"x-api-key": API_KEY}

        # Variables de paginación
        page = 1
        total_pages = 1
        records_inserted = 0
        errors = 0

        # Loop de paginación con manejo de errores
        while page <= total_pages:
            logger.info(f"📄 Solicitando página {page}/{total_pages}")

            params = {"page": page, "limit": 100}
            response_data = fetch_with_retry(url, headers, params)

            if response_data is None:
                errors += 1
                logger.warning(f"⚠️ Fallo al obtener página {page}, continuando...")
                page += 1
                continue

            # Actualizar total de páginas desde metadatos
            meta = response_data.get('meta', {})
            total_pages = meta.get('total_pages', 1)

            # Procesar registros de la página
            for record in response_data.get('data', []):
                try:
                    # Insertar en PostgreSQL usando parámetros para evitar SQL injection
                    sql = """
                        INSERT INTO audit.raw_transactions
                        (transaction_id, user_id, product_category, amount,
                        currency, transaction_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    pg_hook.run(sql, parameters=(
                        record['transaction_id'],
                        record['user_id'],
                        record['product_category'],
                        record['amount'],
                        record['currency'],
                        record['transaction_date'],
                        record['status']
                    ))
                    records_inserted += 1

                except Exception as e:
                    errors += 1
                    logger.error(f"❌ Error insertando registro {record.get('transaction_id')}: {e}")

            page += 1

        # Log de resumen
        logger.info(f"✅ Fase WRITE completada: {records_inserted} registros, {errors} errores")

        return {
            'records_inserted': records_inserted,
            'errors': errors,
            'pages_processed': page - 1
        }

    # -------------------------------------------------------------------------
    # FASE 2: AUDIT - Validación de calidad con Great Expectations
    # -------------------------------------------------------------------------
    @task(task_id='audit_with_gx')
    def audit_with_gx(extract_metrics: Dict[str, int]) -> Dict[str, Any]:
        """
        Ejecuta validaciones en DOS fases:
        1. Suite CRITICO: Si falla -> BLOQUEA (AirflowFailException)
        2. Suite ADVERTENCIA: Si falla -> Registra y continua
        """
        logger.info("Iniciando fase AUDIT: Validacion GX")
        engine = create_engine(DB_URI)

        # Chequeos SQL rapidos
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_users,
                    COUNT(CASE WHEN transaction_id IS NULL THEN 1 END) as null_ids
                FROM audit.raw_transactions
            """))
            null_stats = result.fetchone()
            if null_stats.null_users > 0:
                raise AirflowFailException(f"{null_stats.null_users} registros sin user_id")
            if null_stats.null_ids > 0:
                raise AirflowFailException(f"{null_stats.null_ids} registros sin transaction_id")

        # Suite CRITICO (bloqueante)
        critical = _execute_gx_suite(engine, "transactions_critical_suite")
        if not critical['success']:
            details = "\n".join(critical['failed_expectations'])
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO audit.gx_validation_logs
                    (table_name, expectation_suite_name, total_records,
                    failed_records, success_rate, critical_failures, blocking_triggered)
                    VALUES ('audit.raw_transactions', 'transactions_critical_suite',
                            :total, :failed, :rate, :failures, TRUE)
                """), {
                    'total': critical['total_expectations'],
                    'failed': critical['failed_count'],
                    'rate': critical['success_rate'],
                    'failures': critical['failed_expectations']
                })
            raise AirflowFailException(f"{critical['failed_count']} validaciones criticas fallaron:\n{details}")

        logger.info(f"Suite CRITICO pasado: {critical['passed_count']}/{critical['total_expectations']}")

        # Suite ADVERTENCIA (no bloqueante)
        warning = _execute_gx_suite(engine, "transactions_warning_suite")
        if not warning['success']:
            logger.warning(f"{warning['failed_count']} advertencias fallaron (pipeline continua)")

        total = critical['total_expectations'] + warning['total_expectations']
        failed = critical['failed_count'] + warning['failed_count']
        rate = ((total - failed) / total * 100) if total > 0 else 0

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO audit.gx_validation_logs
                (table_name, expectation_suite_name, total_records,
                failed_records, success_rate, critical_failures)
                VALUES ('audit.raw_transactions', 'COMBINED_SUITES', :total, :failed, :rate, :failures)
            """), {
                'total': total, 'failed': failed, 'rate': rate,
                'failures': warning['failed_expectations'] if not warning['success'] else []
            })

        return {
            'audit_passed': True,
            'critical_suite_success': critical['success'],
            'warning_suite_success': warning['success'],
            'total_expectations': total,
            'total_failed': failed,
            'success_rate': rate
        }


    def _execute_gx_suite(engine, suite_name: str) -> Dict[str, Any]:
        """Ejecuta un suite GX desde archivo JSON."""
        import os, json, inspect, pandas as pd
        from great_expectations.dataset.pandas_dataset import PandasDataset

        paths = [
            f"/opt/airflow/gx/expectations/{suite_name}.json",
            f"gx/expectations/{suite_name}.json",
        ]
        suite_path = next((p for p in paths if os.path.exists(p)), None)
        if not suite_path:
            return {'success': False, 'total_expectations': 0, 'passed_count': 0,
                    'failed_count': 0, 'success_rate': 0,
                    'failed_expectations': [f"Suite no encontrado: {suite_name}"]}

        with open(suite_path, 'r') as f:
            suite_dict = json.load(f)
        expectations = suite_dict.get('expectations', [])
        df = pd.read_sql("SELECT * FROM audit.raw_transactions", engine)

        if len(df) == 0:
            return {'success': False, 'total_expectations': len(expectations),
                    'passed_count': 0, 'failed_count': len(expectations),
                    'success_rate': 0, 'failed_expectations': ["Sin datos"]}

        validator = PandasDataset(df)
        failed, passed = [], []

        for exp in expectations:
            etype = exp.get('expectation_type')
            kwargs = exp.get('kwargs', {}) or {}
            desc = exp.get('meta', {}).get('description', etype)
            sev = exp.get('meta', {}).get('severity', '?')
            try:
                method = getattr(validator, etype, None)
                if not method:
                    raise AttributeError(f"No soportada: {etype}")
                sig = set(inspect.signature(method).parameters.keys())
                result = method(**{k: v for k, v in kwargs.items() if k in sig})
                if result.get('success'):
                    logger.info(f"  [{sev}] {desc}")
                    passed.append(etype)
                else:
                    uc = result.get('result', {}).get('unexpected_count', 0)
                    logger.warning(f"  [{sev}] {desc} ({uc} fallan)")
                    failed.append(f"[{sev}] {desc}: {uc} registros")
            except Exception as e:
                failed.append(f"{etype} (error)")

        rate = (len(passed) / len(expectations) * 100) if expectations else 0
        return {
            'success': len(failed) == 0,
            'total_expectations': len(expectations),
            'passed_count': len(passed),
            'failed_count': len(failed),
            'success_rate': rate,
            'failed_expectations': failed
        }

    # -------------------------------------------------------------------------
    # FASE 3: PUBLISH - Transformación y carga a esquema PROD con Spark
    # -------------------------------------------------------------------------
    @task(task_id='publish_with_spark')
    def publish_with_spark(audit_result) -> Dict[str, int]:
        """
        Usa Spark para transformar datos, aplicar enmascaramiento de PII
        y cargarlos al esquema prod.
        """
        if audit_result is None:
            return {'records_published': 0, 'reason': 'audit_result_missing'}

        if isinstance(audit_result, dict):
            audit_ok = audit_result.get('audit_passed', False)
        else:
            audit_ok = bool(audit_result)

        if not audit_ok:
            logger.error("Auditoria fallida, NO se publican datos")
            return {'records_published': 0, 'reason': 'audit_failed'}

        logger.info(" Iniciando fase PUBLISH: Transformación y enmascaramiento con Spark")

        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import col, lit, current_timestamp
        except ImportError as ie:
            logger.error(" pyspark no está instalado en el entorno de Airflow.")
            raise

        spark = SparkSession.builder \
            .appName("ETL_Publish_WAP_Masked") \
            .master("local[*]") \
            .config("spark.driver.extraClassPath", "/opt/spark-jars/postgresql-42.5.0.jar") \
            .config("spark.executor.extraClassPath", "/opt/spark-jars/postgresql-42.5.0.jar") \
            .getOrCreate()

        try:
            jdbc_properties = {
                "user": DB_CONFIG['user'],
                "password": DB_CONFIG['password'],
                "driver": "org.postgresql.Driver"
            }
            jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

            # Leer datos desde audit
            logger.info(" Leyendo datos desde audit.raw_transactions")
            df_audit = spark.read.jdbc(
                url=jdbc_url,
                table="audit.raw_transactions",
                properties=jdbc_properties
            )
            total_records = df_audit.count()
            logger.info(f" Registros en audit: {total_records}")

            # Limpieza básica
            df_clean = df_audit.filter(
                (col("status") == "COMPLETED") &
                (col("amount") > 0) &
                (col("user_id").isNotNull())
            )

            df_enriched = df_clean.withColumn(
                "data_quality_score", lit(1.0)
            ).withColumn(
                "processed_at", current_timestamp()
            )

            # ========== APLICAR ENMASCARAMIENTO ==========
            try:
                logger.info(" Aplicando enmascaramiento de transaction_id y de transaction_date...")
                rules = load_masking_rules("/opt/airflow/config/masking_rules.json")
                df_masked = apply_masking(df_enriched, rules)
                logger.info(" Enmascaramiento aplicado correctamente")
            except Exception as e:
                logger.error(f" Falló el enmascaramiento: {e}")
                raise

            # Evitar duplicados y cargar a prod
            logger.info(" Preparando datos nuevos para prod.raw_transactions")
            df_prod_existing = spark.read.jdbc(
                url=jdbc_url,
                table="prod.raw_transactions",
                properties=jdbc_properties
            ).select("transaction_id")

            df_to_publish = df_masked.dropDuplicates(["transaction_id"]).join(
                df_prod_existing,
                on="transaction_id",
                how="left_anti"
            )

            new_records = df_to_publish.count()
            duplicate_records = df_masked.dropDuplicates(["transaction_id"]).count() - new_records

            if new_records == 0:
                logger.info(" No hay registros nuevos para publicar.")
                return {
                    'records_published': 0,
                    'records_filtered': total_records,
                    'duplicate_records': duplicate_records,
                    'approval_rate': 0
                }

            logger.info(f"Publicando {new_records} registros enmascarados en prod.raw_transactions")
            df_to_publish.write.jdbc(
                url=jdbc_url,
                table="prod.raw_transactions",
                mode="append",
                properties=jdbc_properties
            )

            published_count = new_records
            filtered_count = total_records - published_count

            logger.info(f"Fase PUBLISH completada con masking:")
            logger.info(f"   • Publicados: {published_count}")
            logger.info(f"   • Filtrados: {filtered_count}")
            logger.info(f"   • Duplicados evitados: {duplicate_records}")

            return {
                'records_published': published_count,
                'records_filtered': filtered_count,
                'duplicate_records': duplicate_records,
                'approval_rate': round(published_count/total_records*100, 2) if total_records > 0 else 0
            }

        finally:
            spark.stop()
            logger.info("Sesión de Spark cerrada")


    # -------------------------------------------------------------------------
    # FASE 4: DBT - Materialización de modelos analíticos
    # -------------------------------------------------------------------------
    @task(task_id='materialize_with_dbt')
    def materialize_with_dbt(publish_metrics: Dict[str, int]) -> bool:
        """
        Ejecuta dbt run para materializar modelos analíticos finales.

        Args:
            publish_metrics: Métricas de la fase de publicación

        Returns:
            True si dbt se ejecutó exitosamente
        """
        import subprocess

        logger.info("🔄 Iniciando fase DBT: Materialización de modelos")

        dbt_dir = "/opt/airflow/dbt/proyecto_unl"
        profiles_dir = "/opt/airflow/dbt"

        # Comando dbt seguro (sin shell=True)
        cmd = ["dbt", "run", "--profiles-dir", profiles_dir, "--target", "prod"]

        try:
            result = subprocess.run(
                cmd,
                cwd=dbt_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minutos máximo
            )

            if result.returncode != 0:
                logger.error("❌ Error ejecutando dbt run:")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                raise AirflowFailException(f"dbt run falló con código {result.returncode}")

            logger.info("✅ dbt run completado exitosamente")
            logger.info(f"📄 Output: {result.stdout[-500:]}")  # Últimos 500 caracteres

            return True

        except subprocess.TimeoutExpired:
            logger.error("⏰ dbt run excedió el tiempo límite de 5 minutos")
            raise
        except FileNotFoundError:
            logger.error("❌ Comando 'dbt' no encontrado. Verificar instalación en Dockerfile")
            raise
        
    

    @task(task_id='monitor_freshness')
    def monitor_freshness() -> Dict[str, Any]:
        """Monitorea si los datos llegaron a tiempo."""
        logger.info("Monitoreando frescura de datos")
        engine = create_engine(DB_URI)

        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT MAX(ingested_at) as last_ingestion
                FROM audit.raw_transactions
                WHERE ingested_at >= NOW() - INTERVAL '24 hours'
            """))
            row = result.fetchone()
            last = row[0]

            if last:
                lag_h = (datetime.now() - last).total_seconds() / 3600
                status = 'FRESH' if lag_h < 2 else ('STALE' if lag_h < 6 else 'MISSING')
            else:
                lag_h = None
                status = 'MISSING'

            conn.execute(text("""
                INSERT INTO audit.data_freshness_monitor
                (table_name, expected_arrival_time, actual_arrival_time,
                freshness_lag, status, threshold_minutes)
                VALUES ('audit.raw_transactions', :expected, :actual, :lag, :status, 120)
            """), {
                'expected': datetime.now() - timedelta(hours=2),
                'actual': last,
                'lag': f'{lag_h} hours' if lag_h else None,
                'status': status
            })

            if status in ['STALE', 'MISSING']:
                logger.warning(f"ALERTA FRESCURA: {status} - {lag_h}h desde ultima ingesta")

            return {'status': status, 'lag_hours': lag_h}

    
    @task(task_id='record_lineage', trigger_rule='all_done')
    def record_lineage(audit_result=None, publish_result=None, dbt_result=None):
        """Registra el linaje de la ejecucion actual."""
        import uuid
        from sqlalchemy import create_engine, text
        import os

        engine = create_engine(DB_URI)
        exec_id = str(uuid.uuid4())[:8]

        nodes = [
            ('API Transacciones', 'source', None, None, 'Fuente externa de datos'),
            ('audit.raw_transactions', 'table', 'audit', 'raw_transactions', 'Datos crudos de API'),
            ('prod.raw_transactions', 'table', 'prod', 'raw_transactions', 'Datos validados con Spark'),
            ('stg_raw_transactions', 'model', 'staging', 'stg_raw_transactions', 'Modelo dbt staging'),
            ('int_transactions_enriched', 'model', 'intermediate', 'int_transactions_enriched', 'Modelo dbt enriquecido'),
            ('fct_transactions', 'model', 'analytics', 'fct_transactions', 'Tabla de hechos'),
            ('dim_users', 'model', 'analytics', 'dim_users', 'Dimension usuarios'),
        ]

        edges = [
            ('API Transacciones', 'audit.raw_transactions', 'Extraccion con paginacion'),
            ('audit.raw_transactions', 'prod.raw_transactions', 'Spark: filtro COMPLETED, amount>0'),
            ('prod.raw_transactions', 'stg_raw_transactions', 'dbt: estandarizacion'),
            ('stg_raw_transactions', 'int_transactions_enriched', 'dbt: metricas historicas'),
            ('int_transactions_enriched', 'fct_transactions', 'dbt: filtro calidad >= 0.7'),
            ('int_transactions_enriched', 'dim_users', 'dbt: agregacion por usuario'),
        ]

        with engine.begin() as conn:
            for name, ntype, schema, table, desc in nodes:
                nid = f"{schema}.{table}" if schema and table else name
                conn.execute(text("""
                    INSERT INTO audit.data_lineage
                    (lineage_type, node_id, node_name, node_type, schema_name, table_name, description, dag_execution_id)
                    VALUES ('node', :nid, :name, :type, :schema, :table, :desc, :exec_id)
                """), {'nid': nid, 'name': name, 'type': ntype, 'schema': schema, 'table': table, 'desc': desc, 'exec_id': exec_id})

            for src, tgt, transform in edges:
                conn.execute(text("""
                    INSERT INTO audit.data_lineage
                    (lineage_type, source_id, target_id, transformation, dag_execution_id)
                    VALUES ('edge', :src, :tgt, :transform, :exec_id)
                """), {'src': src, 'tgt': tgt, 'transform': transform, 'exec_id': exec_id})

        logger.info(f"Linaje registrado: ejecucion {exec_id}")
        return {'execution_id': exec_id}
    
    # -------------------------------------------------------------------------
    # DEFINIR FLUJO DE TAREAS (DAG)
    # -------------------------------------------------------------------------

    # Ejecutar fases en secuencia con paso de métricas entre tareas
    extract_result = write_to_audit()
    freshness = monitor_freshness()
    audit_result = audit_with_gx(extract_result)
    publish_result = publish_with_spark(audit_result)
    dbt_result = materialize_with_dbt(publish_result)
    record_lineage(audit_result, publish_result, dbt_result)

    # El DAG retorna el resultado final para logging
    return {"status": "completed", "timestamp": datetime.now().isoformat()}


# Instanciar el DAG para que Airflow lo registre
dag_instance = taller_etl_unl_wap()
