"""Dump subcommand for exporting Qdrant collection data to CSV."""

import asyncio
import csv
import json
import os
import time
from typing import Optional
from qdrant_client import AsyncQdrantClient
from tqdm import tqdm
import click


async def dump(
    qdrant_api_url: str,
    qdrant_api_key: Optional[str],
    qdrant_collection_name: str,
    output_file: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    include_vectors: bool = True,
    include_payload: bool = True,
    batch_size: int = 500,
    buffer_size: int = 10000,
    id_header: str = "id",
    vector_header: str = "vector",
    payload_header: str = "payload",
):
    """Export Qdrant collection data to CSV format using optimized batch processing."""
    
    qdrant_client = AsyncQdrantClient(
        url=qdrant_api_url, 
        api_key=qdrant_api_key,
        timeout=60.0,
    )

    try:
        # Check if collection exists
        collection_exists = await qdrant_client.collection_exists(collection_name=qdrant_collection_name)
        if not collection_exists:
            raise click.UsageError(
                f"Requested Qdrant collection '{qdrant_collection_name}' does not exist"
            )

        # Get collection info
        collection_info = await qdrant_client.get_collection(
            collection_name=qdrant_collection_name
        )
        vector_dimension = collection_info.config.params.vectors.size
        vector_distance_metric = collection_info.config.params.vectors.distance.lower()
        
        # Get total count
        count_result = await qdrant_client.count(collection_name=qdrant_collection_name)
        total_count = count_result.count
        if total_count == 0:
            raise click.UsageError(
                f"No records present in requested Qdrant collection '{qdrant_collection_name}'"
            )
        
        # Set limit to total count if not specified
        if limit is None:
            limit = total_count
        
        actual_limit = min(limit, total_count)
        
        click.echo(f"🚀 Optimized export of {actual_limit} records from collection '{qdrant_collection_name}'")
        click.echo(f"📁 Output file: {output_file}")
        click.echo(f"🔢 Vector dimension: {vector_dimension}")
        click.echo(f"📏 Distance metric: {vector_distance_metric}")
        click.echo(f"📋 Include vectors: {include_vectors}")
        click.echo(f"📄 Include payload: {include_payload}")
        click.echo(f"📦 Batch size: {batch_size}")
        click.echo(f"💾 Buffer size: {buffer_size}")
        click.echo()
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            click.echo(f"📁 Created output directory: {output_dir}")
        
        # Prepare CSV headers
        headers = [id_header]
        if include_vectors:
            headers.append(vector_header)
        if include_payload:
            headers.append(payload_header)
        
        # Pre-compile JSON serialization for payload
        json_dumps = json.dumps

        # Add elapsed time for the dump process
        start_time = time.time()
        
        # Use buffered writing for better performance
        with open(output_file, 'w', newline='', encoding='utf-8', buffering=buffer_size) as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            # Calculate total batches
            total_batches = (actual_limit + batch_size - 1) // batch_size
            
            # Create progress bar
            with tqdm(total=actual_limit, desc="Exporting", unit=" records") as pbar:
                current_offset = offset or 0
                records_exported = 0
                async def fetch_batch(batch_offset, batch_size_limit):
                    """Fetch a single batch with retry logic."""
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            points, next_offset = await qdrant_client.scroll(
                                collection_name=qdrant_collection_name,
                                limit=batch_size_limit,
                                offset=batch_offset,
                                with_payload=include_payload,
                                with_vectors=include_vectors,
                            )
                            return points, next_offset
                        except Exception as e:
                            if "Message too long" in str(e) and batch_size_limit > 100:
                                # Reduce batch size for gRPC message size issues
                                new_batch_size = batch_size_limit // 2
                                click.echo(f"⚠️ gRPC message too long, reducing batch size from {batch_size_limit} to {new_batch_size}")
                                if new_batch_size >= 100:
                                    return await fetch_batch(batch_offset, new_batch_size)
                            elif attempt < max_retries - 1:
                                click.echo(f"⚠️ Error fetching batch at offset {batch_offset} (attempt {attempt + 1}/{max_retries}): {e}")
                                await asyncio.sleep(1)  # Wait before retry
                                continue
                            else:
                                click.echo(f"❌ Failed to fetch batch at offset {batch_offset} after {max_retries} attempts: {e}")
                                return [], None
                    return [], None
                
                # Process batches sequentially to avoid duplicates
                current_offset = offset or 0
                records_exported = 0
                
                while records_exported < actual_limit:
                    # Calculate current batch size
                    current_batch_size = min(batch_size, actual_limit - records_exported)
                    
                    if current_batch_size <= 0:
                        break
                    
                    # Fetch batch from Qdrant
                    points, next_offset = await fetch_batch(current_offset, current_batch_size)
                    
                    if not points:
                        break
                    
                    # Optimize CSV writing with batch processing
                    rows = []
                    for point in points:
                        row = [point.id]
                        
                        if include_vectors:
                            vector_str = json_dumps(point.vector)
                            row.append(vector_str)
                        
                        if include_payload:
                            payload_str = json_dumps(point.payload) if point.payload else ''
                            row.append(payload_str)
                        
                        rows.append(row)
                    
                    # Write all rows at once
                    writer.writerows(rows)
                    
                    # Update progress
                    records_exported += len(points)
                    pbar.update(len(points))
                    
                    # Update offset for next batch
                    current_offset = next_offset
                    
                    # Check if we've reached the limit
                    if records_exported >= actual_limit:
                        break
                

        
        # Get final file size
        file_size = os.path.getsize(output_file)
        file_size_mb = file_size / (1024 * 1024)
        elapsed_time = time.time() - start_time
        
        click.echo()
        click.echo(f"🚀 Optimized export completed successfully!")
        click.echo(f"📊 Records exported: {records_exported}")
        click.echo(f"📁 File size: {file_size_mb:.2f} MB")
        click.echo(f"📄 Output file: {output_file}")
        click.echo(f"⏱️ Elapsed time: {elapsed_time:.2f} seconds")
        
    finally:
        # Close the async client
        await qdrant_client.close()


def dump_sync(
    qdrant_api_url: str,
    qdrant_api_key: Optional[str],
    qdrant_collection_name: str,
    output_file: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    include_vectors: bool = True,
    include_payload: bool = True,
    batch_size: int = 500,
    buffer_size: int = 10000,
    id_header: str = "id",
    vector_header: str = "vector",
    payload_header: str = "payload",
):
    """Synchronous wrapper for the async dump function."""
    return asyncio.run(dump(
        qdrant_api_url=qdrant_api_url,
        qdrant_api_key=qdrant_api_key,
        qdrant_collection_name=qdrant_collection_name,
        output_file=output_file,
        limit=limit,
        offset=offset,
        include_vectors=include_vectors,
        include_payload=include_payload,
        batch_size=batch_size,
        buffer_size=buffer_size,
        id_header=id_header,
        vector_header=vector_header,
        payload_header=payload_header,
    )) 