import unittest

from stream_generator.event_producer import product_events

class TestEventProducer(unittest.TestCase):

    def test_record_parser(self):
        """
        This will test the correctness of record parser.
        """
        records = ["2,2016-01-01 00:29:24,2016-01-01 00:39:36,N,1,-73.928642272949219,40.680610656738281,-73.924278259277344,40.698043823242188,1,1.46,8,0.5,0.5,1.86,0,,0.3,11.16,1,1\n"]

        product_events(records)
        self.assertFalse(False)


