import csv
from multiprocessing import Pool
import time
from Creatingchunks import equal_chunks, random_chunks
import gc

# Sample Mapper Function
def map_function(data_chunk):
    word_counts = {}
    for word in data_chunk.split():
        word = word.lower()
        word_counts[word] = word_counts.get(word, 0) + 1
    return word_counts

# Reducer Function
def reduce_function(mapped_data):
    reduced_data = {}
    for data in mapped_data:
        for word, count in data.items():
            reduced_data[word] = reduced_data.get(word, 0) + count
    return reduced_data

# Logging Function
def log_results_to_csv(file_path, log_data):
    """Log results into a CSV file."""
    with open(file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(log_data)

# Main MapReduce Function
def mapreduce(input_data, mapper, reducer, num_chunks=4, num_cores=4, chunk_method="equal", log_file="results.csv"):
    """MapReduce function with integrated timing and logging."""
    try:
        # Start total time measurement
        total_start = time.time()

        # Split data into chunks
        if chunk_method == "equal":
            data_chunks = equal_chunks(input_data, num_chunks)
        elif chunk_method == "random":
            data_chunks = random_chunks(input_data, num_chunks)
        else:
            raise ValueError(f"Invalid chunking method: {chunk_method}")

        chunk_sizes = [len(chunk) for chunk in data_chunks]
        avg_chunk_size = sum(chunk_sizes) / len(chunk_sizes) if chunk_sizes else 0

        print(f"Chunking method: {chunk_method}")
        print(f"Number of chunks: {num_chunks}, Number of cores: {num_cores}, Avg chunk size: {avg_chunk_size}")

        # Map Phase Timing
        map_start = time.time()
        with Pool(num_cores) as pool:
            mapped_data = pool.map(mapper, data_chunks)
        map_end = time.time()

        # Reduce Phase Timing
        reduce_start = time.time()
        result = reducer(mapped_data)
        reduce_end = time.time()

        # End total time measurement
        total_end = time.time()

        # Log results
        log_data = [
            num_chunks,        # Number of chunks
            num_cores,         # Number of cores
            chunk_method,      # Chunking method
            avg_chunk_size,    # Average size of chunks
            map_end - map_start,  # Map phase time
            reduce_end - reduce_start,  # Reduce phase time
            total_end - total_start  # Total time
        ]
        log_results_to_csv(log_file, log_data)
        del data_chunks
        gc.collect()

        return result

    except Exception as e:
        print(f"Error during MapReduce execution: {e}")
        return None

# Initialize CSV File
def initialize_csv(file_path):
    """Create a CSV file and write the header row."""
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            "Number of Chunks", "Number of Cores", "Chunking Method", "Average Chunk Size",
            "Map Phase Time (s)", "Reduce Phase Time (s)", "Total Time (s)"
        ])

# Example Usage
if __name__ == "__main__":
    log_file = "results.csv"
    initialize_csv(log_file)  # Create the CSV file with headers

    # Load the input text
    try:
        with open("testFiles/eronEmailDatabase/emails.csv", 'r') as file:
            input_text = file.read()
        if not input_text.strip():
            raise ValueError("Input file is empty.")

        # Experiment with different configurations
        for num_cores in [1,4,8,16]:  # Vary the number of cores
            for num_chunks in [4, 8, 16, 32]:  # Experiment with different numbers of chunks
                print(f"Running MapReduce with {num_chunks} chunks, {num_cores} cores")
                mapreduce(input_text, map_function, reduce_function, num_chunks=num_chunks, num_cores=num_cores, log_file=log_file)

    except FileNotFoundError:
        print("Error: Input file not found.")
    except ValueError as e:
        print(f"Error: {e}")
