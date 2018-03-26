
def get_batch(seq, batch_size):
    start = 0
    end = start + batch_size
    if end >= len(seq):
        end = len(seq)
    batch = seq[start : end]
    del seq[start : end]
    return batch