from pg_export_import import export_and_import
from pg_export_import import ConnectionConfig

export_and_import(source_config=ConnectionConfig(
    host='192.168.10.13',
    dbname='bayvrio_db',
    user='postgres',
    password='password123',
    port=5439
),target_config=ConnectionConfig(
    host='192.168.10.13',
    dbname='bayvrio_db',
    user='postgres',
    password='password123',
    port=5439
),source_table="drug_master",target_table="drug_master1",
where_clause="created_at < %s",
            where_params=("now()",),)