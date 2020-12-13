#!/usr/bin/python3

import json
import re
import bz2
from util import *
from stream import Stream

def read_records(*files):
    """
    Reads all the given files and returns a single stream containing
    all the records (as Python dictionaries) in those files.
    (Files can be bz2-compressed.)
    Converts from JSON but does no other preprocessing.
    """
    return Stream(multi_file_streamer(*files)).map(json.loads)

class Encoder():
    """Expects to process sentences that have already been tokenized."""

    def __init__(self):
        self.w2i = {}
        self.i2w = []
        self.i2count = []

    def __call__(self, sentence):
        """Encode a sentence. Returns the encoded sentence."""
        encoded_word = []
        for word in sentence:
            if word not in self.w2i:
                index = len(self.i2w)
                self.i2w.append(word)
                self.w2i[word] = index
                self.i2count.append(0)
            index = self.w2i[word]
            self.i2count[index] += 1
            encoded_word.append(index)
        return sentence

    def vocab(self):
        """The list of words."""
        return self.i2w

    def word(self, index):
        """Return the word with this index."""
        return self.i2w[index]

    def index(self, word):
        """Return the index of this word."""
        return self.w2i[word]

    def count_index(self, word_index):
        """Return the number of times the word with this index occurred."""
        return self.i2count[word_index]

    def count_word(self, word):
        """Return the number of times this word occurred."""
        return self.count_index(self.index(word))


class StatsAccumulator:
    """A transparent filter."""

    def __init__(self, track_values=False):
        self.fields = {}   # id -> #count
        self.track_values = track_values
        self.field_values = {}   # id -> (value -> #count)

    def __call__(self, record):
        for (key, value) in record.items():
            if key not in self.fields:
                self.fields[key] = 1
                if self.track_values:
                    self.field_values[key] = { value: 1 }
            else:
                self.fields[key] += 1
                if self.track_values:
                    if value not in self.field_values[key]:
                        self.field_values[key][value] = 1
                    else:
                        self.field_values[key][value] += 1
        return record

    def show(self, show_count=False):
        items = sorted(self.fields.items(), key=lambda item: item[1])
        for (key, n) in items:
            if show_count:
                print(key + ': ' + str(n))
            else:
                print(key)
            if self.track_values:
                # TODO: sort by count
                for value in self.field_values[key]:
                    count = self.field_values[key][value]
                    print('  ' + str(value) + ': ' + str(count))

    def get_fields(self):
        return self.fields

    def get_field_values(self):
        return self.fields

def text_length(comment, key='body'):
    return len(comment[key])

def min_text_length(n, key='body'):
    return lambda comment: text_length(comment, key) >= n

def max_text_length(n, key='body'):
    return lambda comment: text_length(comment, key) <= n

def preprocessor_pipeline(funcs):
    return compose(*funcs)

# TODO: add functions for stripping out HTML entities, and maybe URLs?
# TODO: tweak these

def trim_whitespace():
    regex = re.compile(r'\s+')
    return lambda s: re.sub(regex, ' ', s.strip())

def keep_alnum():
    regex = re.compile(r'\W+')
    return lambda s: re.sub(regex, ' ', s)

def strip_digits():
    regex = re.compile(r'\d+')
    return lambda s: re.sub(regex, ' ', s)

# Note: mutates record!
def strip_fields(record, fields):
    keys = set(record.keys())
    for field in keys:
        if field in fields:
            del record[field]
    return record

# Note: mutates record!
def keep_fields(record, fields):
    keys = set(record.keys())
    for field in keys:
        if field not in fields:
            del record[field]
    return record

def show_vocab(encoder, sort_by='index', reverse=False, brief=False):
    if brief:
        words = sum(map(encoder.count_word, encoder.vocab()))
        print('vocab size: ' + str(len(encoder.vocab())))
        print('total word count: ' + str(words))
        return
    if sort_by == 'count':
        key_fun = lambda w: encoder.count_word(w)
    elif sort_by == 'index':
        key_fun = lambda w: encoder.index(w)
    else:
        key_fun = lambda w: w
    vocab = sorted(encoder.vocab(), key = key_fun, reverse=reverse)
    print(', '.join([w + ' = ' + str(encoder.index(w)) + ' (' + str(encoder.count_word(w)) + ')' for w in vocab]))
