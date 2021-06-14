import os
import tensorflow.keras.backend as K
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Embedding, Lambda
import tensorflow.keras.utils as np_utils
import tensorflow as tf


def create_target_sequence_dataset_generator(record_dataset, window_size, vocabulary,
                                             vocabulary_size):
    def generator():
        for raw_record in record_dataset:
            feature = tf.train.Feature()
            feature.ParseFromString(raw_record.numpy())
            sentence = [word.decode('utf-8') for word in feature.bytes_list.value]
            for i in range(window_size, len(sentence) - window_size):
                context_words = []
                target_word = sentence[i]
                for j in range(-window_size, window_size + 1):
                    if j != 0:
                        context_words.append(sentence[i + j])
                x = [vocabulary.get(w, 0) for w in context_words]
                y = np_utils.to_categorical(vocabulary.get(target_word, 0), vocabulary_size)
                yield (x, y)

    return generator


def compile_model(vocab_size, window_size, embed_size=128):
    # build CBOW architecture
    cbow = Sequential()
    cbow.add(Embedding(input_dim=vocab_size, output_dim=embed_size, input_length=window_size * 2))
    cbow.add(Lambda(lambda x: K.mean(x, axis=1), output_shape=(embed_size,)))
    cbow.add(Dense(vocab_size, activation='softmax'))
    cbow.compile(loss='categorical_crossentropy', optimizer='rmsprop')
    return cbow


def create_vocab(filename='db/word_freq.tsv'):
    vocab = None
    vocab_size = 0
    with open(os.path.join(os.getcwd(), filename), 'r') as fdata:
        vocab = {'PAD': 0}
        vocab_size += 1
        for line in fdata:
            parts = line.split()
            if len(parts) == 0:
                break
            if len(parts) != 2:
                print(parts)
                continue
            word, freq = parts
            freq = int(freq)
            if freq < 2000:
                continue
            vocab[word] = vocab_size
            vocab_size += 1

    print('Vocab size: ', vocab_size)
    return vocab, vocab_size


if __name__ == '__main__':
    path = 'db/cbow/tokenized/*'
    window_size = 2
    file_dataset = tf.data.Dataset.list_files(path)
    record_dataset = file_dataset.interleave(lambda filename: tf.data.TFRecordDataset([filename]))

    num_words_processed = 0
    vocab, vocab_size = create_vocab()
    target_sequence_dataset = tf.data.Dataset.from_generator(
        create_target_sequence_dataset_generator(record_dataset, window_size=2, vocabulary=vocab,
                                                 vocabulary_size=vocab_size),
        output_types=(tf.int32, tf.int32),
        output_shapes=((window_size * 2), (vocab_size))
    )

    for x in target_sequence_dataset.take(10):
        print(x)

    cbow = compile_model(vocab_size, window_size)

    # view model summary
    print(cbow.summary())

    model_checkpoint_callback = tf.keras.callbacks.ModelCheckpoint(
        filepath='models/cbow/checkpoints')

    cbow.fit(target_sequence_dataset.batch(1024).prefetch(64), epochs=10,
             callbacks=[model_checkpoint_callback])

    print("Done")
