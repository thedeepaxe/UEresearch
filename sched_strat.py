import csv
from multiprocessing import Pool, Process, Queue, Manager
import time
import random
import gc

# Map Function
def map_function(data_chunk):
    word_counts = {}
    for word in data_chunk.split():
        word = word.lower()
        word_counts[word] = word_counts.get(word, 0) + 1
    return word_counts

# Reduce Function
def reduce_function(mapped_data):
    reduced_data = {}
    for data in mapped_data:
        for word, count in data.items():
            reduced_data[word] = reduced_data.get(word, 0) + count
    return reduced_data

# Logging Function
def log_results_to_csv(file_path, log_data):
    with open(file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(log_data)

# Scheduling Strategies
def assign_chunks_random(chunks):
    random.shuffle(chunks)
    return chunks

def assign_chunks_round_robin(chunks, num_cores):
    assignments = [[] for _ in range(num_cores)]
    for i, chunk in enumerate(chunks):
        assignments[i % num_cores].append(chunk)
    return [chunk for sublist in assignments for chunk in sublist]

# Dynamic Free Core Worker
def dynamic_free_core_worker(queue, result_list, lock):
    while not queue.empty():
        # Get the next available chunk dynamically
        chunk = queue.get()
        result = map_function(chunk)  # Process the chunk
        with lock:
            result_list.append(result)  # Append result in a thread-safe manner

# MapReduce Function with Dynamic Scheduling
def mapreduce(input_data, mapper, reducer, num_chunks=16, num_cores=4, scheduling_strategy="default", log_file="results.csv"):
    total_start = time.time()
    
    # Chunking Data
    chunks = [input_data[i:i + len(input_data) // num_chunks] for i in range(0, len(input_data), len(input_data) // num_chunks)]
    if len(chunks) > num_chunks:
        chunks = chunks[:num_chunks]
    
    # Apply Scheduling Strategy
    if scheduling_strategy == "random":
        chunks = assign_chunks_random(chunks)
    elif scheduling_strategy == "round_robin":
        chunks = assign_chunks_round_robin(chunks, num_cores)
    
    map_start = time.time()
    
    if scheduling_strategy == "free_core":
        queue = Queue()
        # Add chunks to the queue for dynamic allocation
        for chunk in chunks:
            queue.put(chunk)
        
        results = []
        lock = Manager().Lock()  # Lock to ensure thread-safe appending to results
        processes = []
        for _ in range(num_cores):
            p = Process(target=dynamic_free_core_worker, args=(queue, results, lock))
            p.start()
            processes.append(p)
        
        # Wait for all processes to complete
        for p in processes:
            p.join()
        
        mapped_data = results
    else:
        with Pool(num_cores) as pool:
            mapped_data = pool.map(mapper, chunks)
    
    map_end = time.time()
    reduce_start = time.time()
    result = reducer(mapped_data)
    reduce_end = time.time()
    
    total_end = time.time()
    
    # Log Results
    log_data = [
        scheduling_strategy,
        num_chunks,
        num_cores,
        map_end - map_start,
        reduce_end - reduce_start,
        total_end - total_start
    ]
    log_results_to_csv(log_file, log_data)
    
    del chunks
    gc.collect()
    return result

# Initialize CSV File
def initialize_csv(file_path):
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            "Scheduling Strategy", "Number of Chunks", "Number of Cores",
            "Map Phase Time (s)", "Reduce Phase Time (s)", "Total Time (s)"
        ])

# Example Usage
if __name__ == "__main__":
    log_file = "dynamic_scheduling_results.csv"
    initialize_csv(log_file)
    
    with open("testFiles/eronEmailDatabase/emails.csv", 'r') as file:
        input_text = file.read()
    
    for strategy in ["default", "random", "round_robin", "free_core"]:
        print(f"Testing scheduling strategy: {strategy}")
        mapreduce(input_text, map_function, reduce_function, num_chunks=16, num_cores=256, scheduling_strategy=strategy, log_file=log_file)
