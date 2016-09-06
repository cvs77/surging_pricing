from stream_generator.utils.db.simple_dbs import MySQL

from stream_generator.utils.logger import get_logger_by_name


DATA_BASE_NAME = "test_DI"
TABLE_NAME = "test_data"


class StreamDataDbHelper(object):
    """
     The StreamDataDbHelper
    """
    INSERT = u"""
        INSERT INTO `{data_base}`.`{table}` (
        `type`,
        `ts`,
        `lo`,
        `la`,
        `v`,
        `uid`)
        VALUES
        (
        '{type}',
        {ts},
        '{lo}',
        '{la}',
        {v},
        '{uid}'
        ) ;

    """

    LOAD = u"""
        SELECT *
        FROM `{data_base}`.`{table}`
        order by ts
        limit {pos}, {batch_size}
    """

    def __init__(self, db_config):
        self.db_conn = MySQL(db_config)

    @property
    def LOG(self):
        return get_logger_by_name(__name__)

    def insert(self, record_dic):
        record_dic['data_base'] = DATA_BASE_NAME
        record_dic['table'] = TABLE_NAME
        sql = self.INSERT.format(**record_dic)
        self.db_conn.execute(sql)
        self.LOG.info("Insert one record %s", str(record_dic))

    def close_db(self):
        self.db_conn.close_conn()

    def load(self, pos, batch_size):
        """
          Load the event record.
        """
        parameters = {"data_base": DATA_BASE_NAME,
                      "table": TABLE_NAME,
                      "pos": pos,
                      "batch_size": batch_size}
        load_sql = self.LOAD.format(**parameters)
        return self.db_conn.fetch_rows(load_sql)