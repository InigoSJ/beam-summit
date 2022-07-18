import argparse
import json
import logging
import re

import apache_beam as beam

from apache_beam import Create
from apache_beam import DoFn
from apache_beam import Filter
from apache_beam import Map
from apache_beam import ParDo
from apache_beam import pvalue
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.textio import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import DataflowRunner
from apache_beam.transforms.util import Distinct
from apache_beam.transforms.combiners import Count

# TODO
bucket = ""
project = ""
region = ""
# Path to Wordle Words
words = ""
answers = ""
# BigQuery Table
table = ""

def single_letter(word):
    s = set()
    for l in word:
        s.add(l)
    return len(s) == len(word)

class WordleRow(DoFn):
    def process(self, word, answers):
        greens, yellows = 0, 0
        for answer in answers:
            for i, letter in enumerate(word):
                if letter == answer[i]:
                    greens += 1
                elif letter in answer:
                    yellows += 1
        yield self._format_result(word, yellows, greens)

    def _format_result(self, word, yellows, greens):
        return (word, yellows, greens, 1)

def combine_words(main, side_words, size=3):
    def _letter_intersection(main_dict, side_word):
        for l in side_word:
            if l in main_dict:
                return True
        return False

    def _combine_tuples(word, t1, t2):
        return (word, t1[1] + t2[1], t1[2] + t2[2], t1[3] + t2[3])

    main_word = main[0]
    main_dict = {}
    for l in main_word:
        main_dict[l] = 1

    if size == 3:
        list_words = main_word.split(",")

    for side in side_words:
        side_word = side[0]
        intersection = _letter_intersection(main_dict, side_word)

        if not intersection:
            if size == 3:
                words = list_words + [side_word]
                new_word = ",".join(sorted(words))
            elif main_word > side_word:
                new_word = f"{side_word},{main_word}"
            else:
                new_word = f"{main_word},{side_word}"
            yield _combine_tuples(new_word, main, side)

def to_dict(x, total):
    d = {"words": x[0],
         "yellows": x[1],
         "yellow_avg": x[1] / total,
         "greens": x[2],
         "green_avg": x[2] / total,
         "amount_words":x[3],
         "total_words": total}
    return d

def run():
    options = PipelineOptions(
        temp_location=f"{bucket}/tmp/",
        project=project,
        region=region,
        job_name="wordle",
        runner="DataflowRunner",
    )

    p = beam.Pipeline(options=options)

    schema = "words:STRING, yellows:INTEGER, yellow_avg:FLOAT, greens:INTEGER, green_avg:FLOAT, amount_words:INTEGER, total_words:INTEGER"

    accepted_read = p | "Other words" >> ReadFromText(words)
    answers_read = p | "Answers" >> ReadFromText(answers)

    # SIDES
    side_words = ((accepted_read, answers_read) | beam.Flatten()
                                                | Filter(single_letter)
    )

    total_answers = answers_read | Count.Globally()

    # MAIN

    # One
    ones = side_words | "Calculation 1-word" >> ParDo(WordleRow(), answers=beam.pvalue.AsList(answers_read))

    # Two
    twos = (ones
                | "Combine 2-words" >> beam.ParDo(combine_words,
                              side_words=beam.pvalue.AsList(ones),
                              size=2)
                | "Distinct 2-words" >> Distinct())

    # Three
    threes = (twos
                | "Combine 3-words" >> beam.ParDo(combine_words,
                              side_words=beam.pvalue.AsList(ones),
                              size=3)
                | "Distinct 3-words" >> Distinct())

    # Writing to BQ
    flattened = (ones, twos, threes) | "Flatten to BQ" >> beam.Flatten()

    bq = (flattened | Map(to_dict, total=beam.pvalue.AsSingleton(total_answers))
                    | "Write BQ" >> WriteToBigQuery(
                           table,
                           schema=schema,
                           write_disposition=BigQueryDisposition.WRITE_APPEND,
                           create_disposition=BigQueryDisposition.CREATE_IF_NEEDED)
                   )

    p.run()

if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()

