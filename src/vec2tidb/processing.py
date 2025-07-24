import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm


def process_batches_concurrent(
    tasks_total: int, 
    batch_generator,
    batch_processor,
    workers: int = 1,
    batch_size: int = 100,
    *args, 
    **kwargs
) -> int:
    """Generic concurrent batch processing function with progress bar."""
    processed_total = 0
    progress_lock = Lock()
    
    # Create a queue to hold batch data
    batch_queue = queue.Queue(maxsize=workers * 3)  # Buffer for efficiency
    producer_finished = threading.Event()
    
    def producer():
        """Generate batches and put them in the queue."""
        try:
            for batch_data in batch_generator(batch_size=batch_size):
                if not batch_data:
                    break
                batch_queue.put(batch_data)
        except Exception as e:
            print(f"Producer error: {e}")
        finally:
            producer_finished.set()
    
    def worker():
        """Worker function that processes batches from the queue."""
        nonlocal processed_total
        local_processed = 0
        while True:
            try:
                # Get batch from queue with timeout
                batch_data = batch_queue.get(timeout=0.5)
                
                # Process the batch
                result = batch_processor(batch_data)
                if result:
                    with progress_lock:
                        processed_total += result
                        progress_bar.update(result)
                        local_processed += result
                
                batch_queue.task_done()
                
            except queue.Empty:
                # Check if producer is finished and queue is empty
                if producer_finished.is_set() and batch_queue.empty():
                    break
                continue
            except Exception as e:
                print(f"Worker error: {e}")
                break
        
        return local_processed
    
    with tqdm(total=tasks_total) as progress_bar:
        # Start the producer thread
        producer_thread = threading.Thread(target=producer)
        producer_thread.start()
        
        # Start worker threads
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for _ in range(workers):
                future = executor.submit(worker)
                futures.append(future)
            
            # Wait for producer to finish
            producer_thread.join()
            
            # Wait for all workers to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Worker failed: {e}")
    
    return processed_total
