# The Data base config
BATCH_SIZE = 500
DATABASE_CONFIG = {
    'host': "10.160.241.142",
    'type': "mysql",
    'port': '3307',
    'username': 'mozone',
    'password': 'morangerunmozone',
    'database': 'test_DI'
}

# The car speed heart beat in seconds
SPEED_HEART_BEAT = 5 # In every XX seconds the car will report its current speed
# The time zone of the New York
TIME_ZONE = 'EDT'
# The supply waiting time in seconds
REQUEST_WAITING = 8 * 60
# The driver lasting time in seconds
DRIVER_LASTING = 5 * 60






