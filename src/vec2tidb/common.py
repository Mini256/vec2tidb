def process_with_tqdm(
    tasks_total: int, batch_processor, *args, **kwargs
) -> int:
    """Generic data migration function with progress bar."""
    from tqdm import tqdm

    processed_total = 0

    with tqdm(total=tasks_total) as process_bar:
        while True:
            batch_result = batch_processor(*args, **kwargs)
            if not batch_result:
                break

            processed_count, has_more = batch_result
            if not processed_count:
                break

            processed_total += processed_count
            process_bar.update(processed_count)

            if not has_more:
                break

    return processed_total
