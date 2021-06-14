import os, glob, json
from nltk.stem.snowball import SnowballStemmer
import re
import tensorflow as tf
import io


def feature_strings(strings):
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=strings))


class ShardedWriter:
    def __init__(self, num_shards=100, output_dir='data', output_filename='data.tfrecord'):
        self.num_shards = num_shards
        shard_padding = len(str(num_shards - 1))
        self.writers = []
        for i in range(num_shards):
            w = tf.io.TFRecordWriter(
                os.path.join(output_dir, (output_filename + '.' + str(i).zfill(shard_padding))))
            self.writers.append(w)

    def write(self, data):
        serialized = data.SerializeToString()
        shard = hash(serialized) % self.num_shards
        self.writers[shard].write(serialized)

    def close(self):
        for w in self.writers:
            w.close()

    def __enter__(self):
        pass

    def __exit__(self):
        self.close()


def process_file(filename, shard_writer, word_freq):
    if os.path.isdir(filename):
        return False
    with open(os.path.join(os.getcwd(), filename), 'r') as fdata:
        js = json.load(fdata)
        if 'comments_threads' not in js:
            return False
        for item in js['comments_threads']:
            sentence = tokenize(item['text'], stemmer)
            shard_writer.write(feature_strings([w.encode('utf-8') for w in sentence]))
            update_words_frequency(sentence, word_freq)


# Data processing functions
def normalize(word, stemmer):
    return stemmer.stem(word.lower())


def is_valid(word):
    if word.startswith('@'):
        return False
    if '_' in word:
        return False
    if '.' in word:
        return False
    if re.match(r'.*[0-9]', word):
        return False
    if re.fullmatch(r'\s+', word):
        return False
    return True


def tokenize(text, stemmer):
    tokens = []
    for word in text.split():
        if not is_valid(word):
            continue
        for tok in re.findall(r'[\w]+|[\u263a-\U0001f645]|[\U0001F601-\U0001F94F]', text, re.U):
            tokens.append(normalize(tok, stemmer))
    return tokens


def update_words_frequency(sentence, word_freq):
    for word in sentence:
        if word not in word_freq:
            word_freq[word] = 1
        else:
            word_freq[word] += 1


if __name__ == '__main__':
    # Results
    word_freq = {}
    sentences_cleaned = []

    shard_writer = ShardedWriter(num_shards=1000, output_dir='db/cbow/tokenized/')

    # Stemmer
    stemmer = SnowballStemmer('russian')

    # Process and shard YouTube comments
    path = 'db/videos/*'
    files = glob.glob(os.path.join(path))

    print("Num files: ", len(files))

    for f in files:
        process_file(f, shard_writer, word_freq)

    shard_writer.close()

    out = io.open('db/word_freq.tsv', 'w', encoding='utf-8')
    for word, count in word_freq.items():
        out.write(word + "\t" + str(count) + "\n")
    out.close()
