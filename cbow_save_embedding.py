from tensorflow.keras import models
import tensorflow as tf
import io
import re
from cbow_training import create_vocab

if __name__ == '__main__':
    loaded_model = models.load_model('models/cbow/checkpoints', compile=False)
    # CategoricalAccuracy
    # TopKCategoricalAccuracy
    loaded_model.compile(loss='categorical_crossentropy', optimizer='rmsprop',
                         metrics=[tf.keras.metrics.TopKCategoricalAccuracy(k=100)])

    weights = loaded_model.get_layer('embedding').get_weights()[0]
    print(weights.shape)

    vocab, vocab_size = create_vocab()

    out_v = io.open('vectors.tsv', 'w', encoding='utf-8')
    out_m = io.open('metadata.tsv', 'w', encoding='utf-8')

    for word, index in vocab.items():
        if index == 0:
            continue
        if not re.match(r'\w', word):
            continue
        vec = weights[index]
        out_v.write('\t'.join([str(x) for x in vec]) + "\n")
        out_m.write(word + "\n")
    out_v.close()
    out_m.close()
