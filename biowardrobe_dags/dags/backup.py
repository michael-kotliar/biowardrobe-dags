import os
import logging

from sqlparse import split
from contextlib import closing

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

from cwl_airflow_parser.cwlutils import post_status_info


logger = logging.getLogger(__name__)


CONNECTION_ID = "biowardrobe"


def norm_path(path):
    return os.path.abspath(os.path.normpath(os.path.normcase(path)))


def open_file(filename):
    lines = []
    with open(filename, 'r') as infile:
        for line in infile:
            if line.strip():
                lines.append(line.strip())
    return lines


class HookConnect:

    connection_id = None

    def __init__(self, connection_id):
        self.connection_id = connection_id

    def get_conn(self):
        mysql = MySqlHook(mysql_conn_id=self.connection_id)
        return mysql.get_conn()

    def execute(self, sql, option=None):
        with closing(self.get_conn()) as connection:
            with closing(connection.cursor()) as cursor:
                for sql_segment in split(sql):
                    if sql_segment:
                        cursor.execute(sql_segment)
                        connection.commit()
                if option == 1:
                    return cursor.fetchone()
                elif option == 2:
                    return cursor.fetchall()
                else:
                    return None

    def fetchone(self, sql):
        return self.execute(sql,1)

    def fetchall(self, sql):
        return self.execute(sql,2)

    def get_settings_raw(self):
        logger.debug("Get setting raw")
        return {row['key']: row['value'] for row in self.fetchall("SELECT * FROM settings")}

    def get_settings_data(self):
        logger.debug("Get setting data")
        settings = self.get_settings_raw()
        settings_data = {
            "home":     norm_path(settings['wardrobe']),
            "raw_data": norm_path("/".join((settings['wardrobe'], settings['preliminary']))),
            "anl_data": norm_path("/".join((settings['wardrobe'], settings['advanced']))),
            "indices":  norm_path("/".join((settings['wardrobe'], settings['indices']))),
            "upload":   norm_path("/".join((settings['wardrobe'], settings['upload']))),
            "bin":      norm_path("/".join((settings['wardrobe'], settings['bin']))),
            "temp":     norm_path("/".join((settings['wardrobe'], settings['temp']))),
            "experimentsdb": settings['experimentsdb'],
            "airflowdb":     settings['airflowdb'],
            "threads":       settings['maxthreads']
        }
        return settings_data


def backup(**context):
    connect_db = HookConnect(CONNECTION_ID)
    sql_query = """SELECT uid, url
                   FROM labdata
                   WHERE deleted=0 AND
                         libstatus=12 AND
                         url NOT LIKE '%SRR%'"""

    for kwargs in connect_db.fetchall(sql_query):
        try:
            kwargs.update({"raw_data":   raw_data,
                           "peak_type": "broad" if int(kwargs['peak_type']) == 2 else "narrow",
                           "outputs":    loads(kwargs['outputs']) if kwargs['outputs'] and kwargs['outputs'] != "null" else {}})
            kwargs["outputs"]["promoter"] = kwargs["outputs"]["promoter"] if "promoter" in kwargs["outputs"] else 1000
            for template in OUTPUT_TEMPLATES[kwargs['exp_id']][kwargs['peak_type']]:
                kwargs["outputs"].update(fill_template(template, kwargs))
            list(validate_locations(kwargs))
            add_details_to_outputs(kwargs["outputs"])
            connect_db.execute(f"""UPDATE labdata SET params='{dumps(kwargs["outputs"])}' WHERE uid='{kwargs["uid"]}'""")
            logger.info(f"""Update params for {kwargs['uid']}\n {dumps(kwargs["outputs"], indent=4)}""")
        except Exception:
            logger.debug(f"Failed to updated params for {kwargs['uid']}")

dag = DAG(dag_id="backup",
          start_date=days_ago(1),
          on_failure_callback=post_status_info,
          on_success_callback=post_status_info,
          schedule_interval=None)


run_this = PythonOperator(task_id="backup",
                          python_callable=backup,
                          provide_context=True,
                          dag=dag)


