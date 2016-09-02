from stream_generator.utils.db.simple_dbs import MySQL

from stream_generator.utils.logger import get_logger_by_name


class StreamDataDbHelper(object):
    INSERT = u"""
        INSERT INTO `test_DI`.`test_data` (
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
    def __init__(self, db_config):
        self.db_conn = MySQL(db_config)

    @property
    def LOG(self):
        return get_logger_by_name(__name__)

    def insert(self, record_dic):
        sql = self.INSERT.format(**record_dic)
        self.db_conn.execute(sql)
        self.LOG.info("Insert one record %s", str(record_dic))

    def close_db(self):
        self.db_conn.close_conn()
