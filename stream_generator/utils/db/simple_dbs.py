import _mysql
import copy
from datetime import datetime
import time

from stream_generator.utils.logger import get_logger_by_name
logger = get_logger_by_name(__name__)

'''
Homage:
http://mysql-python.sourceforge.net/

MySQLdb User Guide:    
http://mysql-python.sourceforge.net/MySQLdb.html

Download:
http://sourceforge.net/projects/mysql-python/

Package: 
    MySQL-python-1.2.3.win32-py2.7.msi
    MySQL-python-1.2.3.tar.gz
'''


def split_collection(collection, split_size):
    '''Splits a collection into many batches.

    Args:
        collection (iterable): a collection of items.
        split_size (int): the split size.
    
    Returns:
        A generator of a list.   
    '''
    #    assert(split_size > 0)
    (i, batch) = (0, [])  # initialize
    for e in collection:
        batch.append(e)
        i += 1
        if i >= split_size:
            yield batch
            (i, batch) = (0, [])  # re-initialize
    if len(batch) > 0:  # return the rest
        yield batch


def stringnize(collection):
    '''Turn a collection into a string without any brackets.
       
       Collection can be: tuple, list, generator. Elemenet should be int or str.
       \ and ' will be converted to \\ and \' respectively.
    '''
    escape = (str(e).replace('\\', '\\\\').replace("'", "\\'") for e in collection)
    return ",".join("'{0}'".format(e) for e in escape)


def escape_string(s):
    return _mysql.escape_string(s)


def reconnect(func):
    def _func(self, sql):
        try:
            return func(self, sql)
        except Exception as e:
            err_code = e.args[0]
            if (err_code == 0 or err_code == 2006 or err_code == 2013):
                time.sleep(5)  # sleep 5 seconds
                print('{0} Reopen MySQL Connection'.format(datetime.now()), e)
                eval('self.open_conn()')  # re open
                return func(self, sql)
            else:
                logger.error('wrong sql=%s', sql)
                raise e

    _func.__doc__ = func.__doc__
    _func.__name__ = func.__name__
    return _func


from host.utils.config import BATCH_SIZE

_DEFAULT_BATCH_SIZE = BATCH_SIZE
_DEFAULT_SLEEP_TIME = 0  # seconds


class DB(object):
    '''DB API Interface.
    
    Inheritance:
        1 __init__: provide the config.
        2 Implement: fetch_row, fetch_rows, execute.
    
    Dependency:
        1 row -> scalar -> number
        2 normal node: rows -> set, dict
        3 batch mode: rows -> row_batch -> set_batch, dict_batch
    
    Configuration Parameters:
    Name            Type           Meaning                            Default
    batch_size      int            batch size                           1000
    sleep_time      float        sleep seconds in batch mode             0
    
    Currently:
        1 support key based fields (position based is not supported) 
        2 use generator for large rows
    
    Name        Type                         Default
    scalar     scalar                        None
    row        dictionary                    dict()  {} 
    rows       list of dictionary            list()  []
    set        set                           set()         
    row_batch  a generator of rows           
    set_batch 
    dict_batch
    
    Functions:
    config: copy_config
    connection: open_conn, close_conn, is_open
    content: execute, fetch*
    '''

    def __init__(self, config):
        'The config must be provided by its children.'
        self.config = config
        self._verbose = False

    def copy_config(self):
        'Returns a deep copy of the configuration.'
        return copy.deepcopy(self.config)

    def set_verbose(self, verbose):
        assert (type(verbose) == bool)
        self._verbose = verbose

    def get_verbose(self):
        return self._verbose

    def open_conn(self):
        'Opens a connection.'
        raise NotImplementedError()

    def is_open(self):
        'Check whether the connection is still open.'
        raise NotImplementedError()

    def close_conn(self):
        'Close the connection.'
        raise NotImplementedError()

    def execute(self, sql):
        'Executes a command.'
        raise NotImplementedError()

    def fetch_row(self, sql):
        'Returns a row as a dictionary.'
        raise NotImplementedError()

    def fetch_rows(self, sql):
        'Returns rows as a list of dictionaries.'
        raise NotImplementedError()

    def fetch_scalar(self, sql):
        'Returns a scalar (str) or None.'
        row = self.fetch_row(sql)
        return row.values()[0] if row else None

    def fetch_number(self, sql):
        'Returns an integer or a floating number or an exception.'
        scalar = self.fetch_scalar(sql)
        try:
            return int(scalar)
        except:
            return float(scalar)

    def fetch_set(self, sql):
        'Returns a set of a random field in each row.'
        return set(row.values()[0] for row in self.fetch_rows(sql) if row)

    def fetch_dict(self, sql, key, val):
        'Returns a dictionary of with the given key and value fields.'
        assert (key and val)
        return dict((row[key], row[val]) for row in self.fetch_rows(sql))

    def fetch_row_batch(self, sql_template, collection,
                        batch_size=None, sleep_time=None):
        '''Returns rows as a generator of dictionaries using batch mode. 
        
        Args:
            sql_template: the SQL template, must contain ({0}).
            collection: a collection of items.
            batch_size (int): the number of rows sent to database at each batch.
            sleep_time (float): sleep time in seconds
        
        Returns:
            a generator that returns a row each time.
             
        sql = sql_template.format(str(batch))
        select * from table where field in ({batches}).
        '''
        batch_size = _DEFAULT_BATCH_SIZE if batch_size is None else batch_size
        assert (batch_size > 0)  # must be positive

        sleep_time = _DEFAULT_SLEEP_TIME if sleep_time is None else sleep_time
        assert (sleep_time >= 0)  # must be a non-negative number

        for batch in split_collection(collection, batch_size):
            time.sleep(sleep_time)
            for row in self.fetch_rows(sql_template.format(stringnize(batch))):
                yield row

    def fetch_set_batch(self, sql_template, collection,
                        batch_size=None, sleep_time=None):
        '''Returns a set of a random field in each row using batch mode.
        
        Refer to fetch_row_batch.
        '''
        return set(row.values()[0] for row in
                   self.fetch_row_batch(sql_template, collection, batch_size, sleep_time))

    def fetch_dict_batch(self, sql_template, collection, key, val,
                         batch_size=None, sleep_time=None):
        ''''Returns a dictionary with the given key and value using batch mode.
        
        Refer to fetch_row_batch and fetch_dict.
        '''
        return dict((row[key], row[val]) for row in
                    self.fetch_row_batch(sql_template, collection, batch_size, sleep_time))


class MySQL(DB):
    def __init__(self, config):
        DB.__init__(self, config)
        self.conn = None
        self.open_conn()

    def open_conn(self):
        if 'port' not in self.config:
            self.config['port'] = '3306'  # default port
        if 'database' not in self.config:
            self.config['database'] = ''  # no database
        self.conn = _mysql.connect(host=self.config['host'],
                                   port=int(self.config['port']),
                                   user=self.config['username'],
                                   passwd=self.config['password'],
                                   db=self.config['database'])

    def is_open(self):
        return True if self.conn and self.conn.open else False

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __enter__(self):
        pass

    def close_conn(self):
        'Close the connection.'
        if self.conn:
            self.conn.close()

    def set_charset(self, charset='utf8'):
        # self.conn.query("set names {0}".format(charset))
        self.execute("set names {0}".format(charset))

    @reconnect
    def execute(self, sql):
        self.conn.query(sql)
        return self.conn.affected_rows()

    @reconnect
    def fetch_row(self, sql):
        self.conn.query(sql)
        rs = self.conn.store_result()
        row = rs.fetch_row(how=1, maxrows=1)
        return row[0] if row else {}

    @reconnect
    def fetch_rows(self, sql):
        self.conn.query(sql)
        rs = self.conn.store_result()
        return [row for row in rs.fetch_row(how=1, maxrows=0)]


