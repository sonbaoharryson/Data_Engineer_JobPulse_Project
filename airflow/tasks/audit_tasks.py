import os
import logging
import traceback
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from scripts.utils.db_conn import DBConnection

logger = logging.getLogger(__name__)

class AuditLogger:
    """Utility class for logging audit data to audit.master_job_elt_audit table"""
    
    def __init__(self):
        self.db_conn = DBConnection()
        self.engine = self.db_conn.engine
    
    def _determine_task_type(self, task_id: str) -> str:
        """Determine task type from task_id"""
        if 'bash' in task_id.lower() or 'dbt' in task_id.lower():
            return 'bash'
        elif 'post' in task_id.lower() or 'discord' in task_id.lower():
            return 'python'
        elif 'scrape' in task_id.lower() or 'crawl' in task_id.lower():
            return 'python'
        elif 'insert' in task_id.lower():
            return 'python'
        else:
            return 'python'
    
    def _determine_data_source(self, task_id: str) -> Optional[str]:
        """Determine data source from task_id"""
        task_id_lower = task_id.lower()
        if 'itviec' in task_id_lower:
            return 'itviec'
        elif 'topcv' in task_id_lower:
            return 'topcv'
        elif 'bronze' in task_id_lower:
            return 'bronze'
        elif 'silver' in task_id_lower:
            return 'silver'
        elif 'gold' in task_id_lower:
            return 'gold'
        return None
    
    def _determine_layer(self, task_id: str) -> Optional[str]:
        """Determine data layer from task_id"""
        task_id_lower = task_id.lower()
        if 'bronze' in task_id_lower:
            return 'bronze'
        elif 'silver' in task_id_lower:
            return 'silver'
        elif 'gold' in task_id_lower:
            return 'gold'
        elif 'staging' in task_id_lower:
            return 'staging'
        return None


    def log_task_execution(
        self,
        context: Dict[str, Any],
        task_status: str,
        rows_processed: int = 0,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        rows_scraped: int = 0,
        rows_posted_discord: int = 0,
        discord_posts_failed: int = 0,
        dbt_models_run: int = 0,
        dbt_models_success: int = 0,
        dbt_models_failed: int = 0,
        dbt_command: Optional[str] = None,
        error_message: Optional[str] = None,
        error_type: Optional[str] = None,
        additional_metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log task execution to audit table
        
        Args:
            context: Airflow task context (from task callbacks or task instance)
            task_status: Task status ('success', 'failed', 'skipped', 'retry')
            rows_processed: Number of rows processed
            rows_inserted: Number of rows inserted
            rows_updated: Number of rows updated
            rows_scraped: Number of rows scraped (for crawling tasks)
            rows_posted_discord: Number of Discord posts sent
            discord_posts_failed: Number of failed Discord posts
            dbt_models_run: Number of dbt models run
            dbt_models_success: Number of successful dbt models
            dbt_models_failed: Number of failed dbt models
            dbt_command: Full dbt command executed
            error_message: Error message if task failed
            error_type: Type of error
            additional_metadata: Additional metadata to include
        """
        try:
            # Extract context information
            dag_run = context.get('dag_run')
            task_instance = context.get('task_instance')
            dag = context.get('dag')
            
            # Get DAG run information
            dag_run_id = dag_run.run_id if dag_run else None
            execution_date = dag_run.logical_date if dag_run else context.get('execution_date')
            logical_date = dag_run.logical_date if dag_run else execution_date
            run_type = dag_run.run_type if dag_run else 'scheduled'
            
            # Get task information
            if task_instance:
                task_id = task_instance.task_id
                task_name = task_id
                # Try to get task group from task_instance
                task_group = getattr(task_instance, 'group_name', None)
                if not task_group and hasattr(task_instance, 'task') and hasattr(task_instance.task, 'group_id'):
                    task_group = task_instance.task.group_id
            else:
                # Fallback: try to extract from context
                task_instance_key = context.get('task_instance_key_str', '')
                if '__' in task_instance_key:
                    task_id = task_instance_key.split('__')[1]
                else:
                    task_id = context.get('task', {}).task_id if context.get('task') else ''
                task_name = task_id
                task_group = None
            
            # Determine task type and data source from task_id
            task_type = self._determine_task_type(task_id)
            data_source = self._determine_data_source(task_id)
            layer = self._determine_layer(task_id)
            
            # Get execution times
            task_start_date = task_instance.start_date if task_instance else None
            task_end_date = task_instance.end_date if task_instance else datetime.now()
            task_duration = None
            if task_start_date and task_end_date:
                task_duration = int((task_end_date - task_start_date).total_seconds())
            
            # Get retry count
            task_retry_count = task_instance.try_number - 1 if task_instance else 0
            
            # Get error information
            if error_message is None and task_status == 'failed':
                error_message = str(context.get('exception', ''))
                if task_instance and hasattr(task_instance, 'exception'):
                    error_message = str(task_instance.exception)
            
            if error_type is None and error_message:
                error_type = type(context.get('exception', Exception())).__name__
            
            exception_traceback = None
            if task_status == 'failed' and context.get('exception'):
                exception_traceback = traceback.format_exc()
            
            # Get log URL (Airflow task log URL)
            log_url = None
            if dag_run and task_instance:
                try:
                    log_url = dag_run.get_task_instance(context["task_instance"].task_id).log_url
                except:
                    pass
            
            # Get executor
            executor = 'LocalExecutor'
            
            # Get operator
            operator = None
            if task_instance and hasattr(task_instance, 'operator'):
                operator = str(type(task_instance.operator).__name__)
            elif task_instance and hasattr(task_instance, 'task'):
                operator = str(type(task_instance.task).__name__)
            
            # Get Discord channel ID if applicable
            discord_channel_id = os.getenv('DISCORD_CHANNEL_ID') if rows_posted_discord > 0 else None
            
            # Merge additional metadata
            if additional_metadata:
                if 'data_source' in additional_metadata:
                    data_source = additional_metadata['data_source']
                if 'layer' in additional_metadata:
                    layer = additional_metadata['layer']
                if 'task_group' in additional_metadata:
                    task_group = additional_metadata['task_group']
            
            # Insert audit record
            insert_query = text("""
                INSERT INTO audit.master_job_elt_audit (
                    dag_run_id, dag_id, execution_date, logical_date, run_type,
                    dag_status, start_date, end_date, duration_seconds,
                    task_id, task_name, task_status, task_start_date, task_end_date,
                    task_duration_seconds, task_retry_count,
                    task_group, task_type, data_source, layer,
                    rows_processed, rows_inserted, rows_updated, rows_scraped, rows_posted_discord,
                    dbt_models_run, dbt_models_success, dbt_models_failed, dbt_command,
                    error_message, error_type, log_url, exception_traceback,
                    discord_posts_sent, discord_posts_failed, discord_channel_id,
                    executor, operator, updated_at
                ) VALUES (
                    :dag_run_id, :dag_id, :execution_date, :logical_date, :run_type,
                    :dag_status, :start_date, :end_date, :duration_seconds,
                    :task_id, :task_name, :task_status, :task_start_date, :task_end_date,
                    :task_duration_seconds, :task_retry_count,
                    :task_group, :task_type, :data_source, :layer,
                    :rows_processed, :rows_inserted, :rows_updated, :rows_scraped, :rows_posted_discord,
                    :dbt_models_run, :dbt_models_success, :dbt_models_failed, :dbt_command,
                    :error_message, :error_type, :log_url, :exception_traceback,
                    :discord_posts_sent, :discord_posts_failed, :discord_channel_id,
                    :executor, :operator, CURRENT_TIMESTAMP
                )
                ON CONFLICT (dag_run_id, task_id, execution_date) 
                DO UPDATE SET
                    task_status = EXCLUDED.task_status,
                    task_end_date = EXCLUDED.task_end_date,
                    task_duration_seconds = EXCLUDED.task_duration_seconds,
                    task_retry_count = EXCLUDED.task_retry_count,
                    rows_processed = EXCLUDED.rows_processed,
                    rows_inserted = EXCLUDED.rows_inserted,
                    rows_updated = EXCLUDED.rows_updated,
                    rows_scraped = EXCLUDED.rows_scraped,
                    rows_posted_discord = EXCLUDED.rows_posted_discord,
                    dbt_models_run = EXCLUDED.dbt_models_run,
                    dbt_models_success = EXCLUDED.dbt_models_success,
                    dbt_models_failed = EXCLUDED.dbt_models_failed,
                    error_message = EXCLUDED.error_message,
                    error_type = EXCLUDED.error_type,
                    exception_traceback = EXCLUDED.exception_traceback,
                    discord_posts_sent = EXCLUDED.discord_posts_sent,
                    discord_posts_failed = EXCLUDED.discord_posts_failed,
                    updated_at = CURRENT_TIMESTAMP
            """)
            
            params = {
                'dag_run_id': dag_run_id,
                'dag_id': dag.dag_id if dag else 'master_job_elt',
                'execution_date': execution_date,
                'logical_date': logical_date,
                'run_type': str(run_type) if run_type else 'scheduled',
                'dag_status': task_status,  # For task-level, this represents task status
                'start_date': task_start_date,
                'end_date': task_end_date,
                'duration_seconds': task_duration,
                'task_id': task_id,
                'task_name': task_name,
                'task_status': task_status,
                'task_start_date': task_start_date,
                'task_end_date': task_end_date,
                'task_duration_seconds': task_duration,
                'task_retry_count': task_retry_count,
                'task_group': task_group,
                'task_type': task_type,
                'data_source': data_source,
                'layer': layer,
                'rows_processed': rows_processed,
                'rows_inserted': rows_inserted,
                'rows_updated': rows_updated,
                'rows_scraped': rows_scraped,
                'rows_posted_discord': rows_posted_discord,
                'dbt_models_run': dbt_models_run,
                'dbt_models_success': dbt_models_success,
                'dbt_models_failed': dbt_models_failed,
                'dbt_command': dbt_command,
                'error_message': error_message[:5000] if error_message else None,  # Limit size
                'error_type': error_type,
                'log_url': log_url,
                'exception_traceback': exception_traceback[:10000] if exception_traceback else None,  # Limit size
                'discord_posts_sent': rows_posted_discord,
                'discord_posts_failed': discord_posts_failed,
                'discord_channel_id': discord_channel_id,
                'executor': executor,
                'operator': operator
            }
            
            if self.engine:
                with self.engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        conn.execute(insert_query, params)
                        trans.commit()
                        logger.info(f"Audit log inserted for task {task_id} with status {task_status}")
                    except SQLAlchemyError as e:
                        trans.rollback()
                        logger.error(f"Error inserting audit log: {e}")
            else:
                logger.error("Database connection not available for audit logging")
                
        except Exception as e:
            logger.error(f"Error in audit logging: {e}")
            logger.error(traceback.format_exc())
    

def get_audit_logger() -> AuditLogger:
    """Factory function to get AuditLogger instance"""
    return AuditLogger()


def task_success_callback(context):
    """Log successful task execution to audit table"""
    
    audit_logger = get_audit_logger()
    
    ti = context.get('ti')
    
    return_value = ti.xcom_pull(key='return_value', default={})
    
    if isinstance(return_value, dict):
        rows_processed = return_value.get('rows_processed', 0)
        rows_inserted = return_value.get('rows_inserted', 0)
        rows_scraped = return_value.get('rows_scraped', 0) if not isinstance(return_value.get('rows_scraped', list), list) else len(return_value.get('rows_scraped')) 
        posts_sent = return_value.get('posts_sent', 0)
    else:
        rows_processed = 0
        rows_inserted = 0
        rows_scraped = 0
        posts_sent = 0
    
    # Log to audit table  
    audit_logger.log_task_execution(
        context=context,
        task_status='success',
        rows_processed=rows_processed,
        rows_inserted=rows_inserted,
        rows_scraped=rows_scraped,
        rows_posted_discord=posts_sent
    )

def task_failure_callback(context):
    """Log failed task execution to audit table"""
    
    audit_logger = get_audit_logger()
    
    error_message = str(context.get('exception', ''))
    error_type = type(context.get('exception', Exception())).__name__

    audit_logger.log_task_execution(
        context=context,
        task_status='failed',
        error_message=error_message,
        error_type=error_type
    )

def dbt_task_callback(context):
    """Log dbt task execution to audit table"""
    
    audit_logger = get_audit_logger()
    
    task_id = context.get('task_instance').task_id
    layer = None
    
    if 'bronze' in task_id.lower():
        layer = 'bronze'
    elif 'silver' in task_id.lower():
        layer = 'silver'
    elif 'gold' in task_id.lower():
        layer = 'gold'
    
    dbt_command = f'dbt run --select {layer}'

    audit_logger.log_task_execution(
        context=context,
        task_status='success',
        dbt_command=dbt_command,
        additional_metadata={
            'layer': layer,
            'task_group': 'dbt_wh_pipeline'
        }
    )

def discord_task_callback(context):
    """Log discord task to audit table"""
    
    audit_logger = get_audit_logger()
    
    ti = context.get('ti')
    return_value =ti.xcom_pull(key='return_value', default={})
    
    posts_sent = return_value.get('posts_sent', 0) if isinstance(return_value, dict) else 0
    posts_failed = return_value.get('posts_failed', 0) if isinstance(return_value, dict) else 0
    
    # Determine data source from task_id
    task_id = context.get('task_instance').task_id if context.get('task_instance') else ''
    data_source = 'itviec' if 'itviec' in task_id.lower() else 'topcv' if 'topcv' in task_id.lower() else None
    
    audit_logger.log_task_execution(
        context=context,
        task_status='success',
        rows_posted_discord=posts_sent,
        discord_posts_failed=posts_failed,
        additional_metadata={
            'data_source': data_source
        }
    )