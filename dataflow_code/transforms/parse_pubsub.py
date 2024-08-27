import apache_beam as beam
import json

class ParsePubSubMessage(beam.DoFn):
    def process(self, element, *args, **kwargs):
        row = json.loads(element)
        yield row