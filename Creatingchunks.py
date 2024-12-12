import random

def equal_chunks(data, num_chunks):
    """Divide data into equal-sized chunks."""
    chunk_size = len(data) // num_chunks
    return [data[i * chunk_size: (i + 1) * chunk_size] for i in range(num_chunks)]

def random_chunks(data, num_chunks):
    """Divide data into random-sized chunks."""
    chunk_sizes = [random.randint(1, len(data) // 2) for _ in range(num_chunks - 1)]
    chunk_sizes.append(len(data) - sum(chunk_sizes))  # Adjust the last chunk size
    random.shuffle(chunk_sizes)  # Shuffle chunk sizes for randomness
    
    chunks = []
    start = 0
    for size in chunk_sizes:
        chunks.append(data[start:start + size])
        start += size
    return chunks
