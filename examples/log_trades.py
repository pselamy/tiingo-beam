import os

import apache_beam as beam
from apache_beam.options import pipeline_options

from tiingo_beam import endpoints
from tiingo_beam import firehose

if __name__ == "__main__":
    with beam.Pipeline(options=pipeline_options.StandardOptions()) as pipeline:
        (
            pipeline
            | firehose.GetTrades(
                api_key=os.environ["TIINGO_API_KEY"],
                endpoints=(endpoints.Endpoint.CRYPTO,),
                # A "thresholdLevel" of 2 means you will get Top-of-Book AND Last Trade updates.
                # A "thresholdLevel" of 5 means you will get only Last Trade updates.
                threshold_level=5,
            )
            | "Print" >> beam.Map(print)
        )
