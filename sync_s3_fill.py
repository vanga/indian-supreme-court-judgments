"""
S3 Gap-Filling Module for Supreme Court Judgments
Handles filling gaps in historical data by processing 5-year chunks
"""

import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from tqdm import tqdm

from archive_manager import S3ArchiveManager

logger = logging.getLogger(__name__)

START_DATE = "1950-01-01"


def get_fill_progress_file():
    """Get path to the fill progress tracking file"""
    return Path("./sc_fill_progress.json")


def save_fill_progress(
    start_date,
    end_date,
    completed_chunks,
    last_chunk_end,
    completed_years_in_current_chunk,
    current_chunk,
):
    """Save progress for gap filling process"""
    progress_data = {
        "start_date": start_date,
        "end_date": end_date,
        "completed_chunks": completed_chunks,
        "last_chunk_end": last_chunk_end,
        "completed_years_in_current_chunk": list(completed_years_in_current_chunk),
        "current_chunk": current_chunk,
        "last_updated": datetime.now().isoformat(),
    }

    with open(get_fill_progress_file(), "w") as f:
        json.dump(progress_data, f, indent=2)

    logger.info(f"Progress saved: last completed chunk ending at {last_chunk_end}")


def load_fill_progress():
    """Load existing progress for gap filling process"""
    progress_file = get_fill_progress_file()
    if not progress_file.exists():
        return None

    try:
        with open(progress_file, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load progress file: {e}")
        return None


def clear_fill_progress():
    """Clear progress file when process completes"""
    progress_file = get_fill_progress_file()
    if progress_file.exists():
        progress_file.unlink()
        logger.info("Progress file cleared - gap filling completed")


def generate_five_year_chunks(start_date, end_date):
    """Generate 5-year chunks between start and end dates"""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    all_chunks = []
    current_chunk_start = start_dt

    while current_chunk_start <= end_dt:
        # Calculate 5-year chunk end
        chunk_end = min(
            datetime(current_chunk_start.year + 5, 1, 1) - timedelta(days=1), end_dt
        )

        chunk_tuple = (
            current_chunk_start.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d"),
        )
        all_chunks.append(chunk_tuple)

        # Move to next chunk
        current_chunk_start = datetime(current_chunk_start.year + 5, 1, 1)

    return all_chunks


def sync_s3_fill_gaps(
    s3_bucket,
    s3_prefix,
    local_dir,
    start_date=None,
    end_date=None,
    day_step=30,
    max_workers=5,
    timeout_hours=None,
):
    """
    Fill gaps in S3 data by processing ONE 5-year chunk per run.
    Uses immediate upload after each year to prevent data loss.
    Run this repeatedly - it will automatically pick up the next 5-year chunk each time.

    Args:
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix for data
        local_dir: Local directory for temporary files
        start_date: Start date for gap analysis (defaults to START_DATE = 1950-01-01)
        end_date: End date for gap analysis (defaults to current date)
        day_step: DEPRECATED - kept for compatibility, always uses daily tasks (1-day step)
        max_workers: Number of parallel workers
        timeout_hours: Maximum hours to run before graceful exit (default None = no timeout)
    """
    import concurrent.futures

    from download import generate_tasks, process_task
    from process_metadata import SupremeCourtS3Processor

    start_time = time.time()
    timeout_seconds = timeout_hours * 3600 if timeout_hours else None

    logger.info("🚀 Starting 5-year chunk S3 gap-filling process...")
    if timeout_hours:
        logger.info(
            f"⏰ Timeout set to {timeout_hours} hours ({timeout_seconds / 60:.0f} minutes)"
        )
    else:
        logger.info("⏰ No timeout - will run until completion")

    # Check for existing progress
    existing_progress = load_fill_progress()

    # Determine the overall start and end dates
    overall_start = start_date or START_DATE
    overall_end = end_date or datetime.now().strftime("%Y-%m-%d")

    if existing_progress:
        logger.info("📋 Found existing progress from previous run:")
        logger.info(
            f"  Overall range: {existing_progress['start_date']} to {existing_progress['end_date']}"
        )
        logger.info(
            f"  Last completed chunk: {existing_progress.get('last_chunk_end', 'None')}"
        )
        logger.info(
            f"  Completed chunks: {len(existing_progress.get('completed_chunks', []))}"
        )

        # Use existing overall range
        overall_start = existing_progress["start_date"]
        overall_end = existing_progress["end_date"]
        completed_chunks = [
            tuple(c) for c in existing_progress.get("completed_chunks", [])
        ]
    else:
        completed_chunks = []

    logger.info(f"📅 Overall processing range: {overall_start} to {overall_end}")

    # Generate 5-year chunks
    all_five_year_chunks = generate_five_year_chunks(overall_start, overall_end)

    # Filter out completed chunks
    remaining_chunks = [c for c in all_five_year_chunks if c not in completed_chunks]

    logger.info(f"📊 Total 5-year chunks: {len(all_five_year_chunks)}")
    logger.info(f"✅ Already completed: {len(completed_chunks)}")
    logger.info(f"⏳ Remaining chunks: {len(remaining_chunks)}")

    if not remaining_chunks:
        logger.info("🎉 All chunks completed! Clearing progress file.")
        clear_fill_progress()
        return

    # Process ALL remaining chunks in this run
    for chunk_index, (chunk_start, chunk_end) in enumerate(remaining_chunks):
        print()
        logger.info(f"{'=' * 70}")
        logger.info(
            f"📦 Processing chunk {len(completed_chunks) + chunk_index + 1}/{len(all_five_year_chunks)}: {chunk_start} to {chunk_end}"
        )
        logger.info(f"{'=' * 70}")
        logger.info("")

        # Check timeout before starting a new chunk
        if timeout_seconds and time.time() - start_time >= timeout_seconds:
            logger.warning(
                f"⏰ Timeout reached before starting chunk {chunk_start} to {chunk_end}"
            )
            logger.info("💾 Progress saved. Run again to continue from this chunk.")
            return

        # Check if we're resuming an incomplete chunk
        completed_years_in_chunk = set()
        if existing_progress and existing_progress.get("current_chunk") == [
            chunk_start,
            chunk_end,
        ]:
            completed_years_in_chunk = set(
                existing_progress.get("completed_years_in_current_chunk", [])
            )
            if completed_years_in_chunk:
                logger.info(
                    f"📋 Resuming incomplete chunk. Already completed years: {sorted(completed_years_in_chunk)}"
                )

        # Track years processed in this chunk
        years_in_chunk = completed_years_in_chunk.copy()
        current_year = None
        chunk_changes = {}
        upload_metadata = {}

        # Use immediate upload mode
        with S3ArchiveManager(
            s3_bucket, s3_prefix, local_dir, immediate_upload=True
        ) as archive_manager:
            loop_completed_successfully = False
            try:
                # Generate date-based tasks for this chunk
                # CRITICAL: Use day_step=1 for daily tasks (not the parameter value)
                # The parameter is kept for backward compatibility but ignored
                all_tasks = list(generate_tasks(chunk_start, chunk_end, day_step=1))

                # Filter out tasks from already-completed years
                tasks = [
                    task
                    for task in all_tasks
                    if datetime.strptime(task.from_date, "%Y-%m-%d").year
                    not in completed_years_in_chunk
                ]

                skipped_count = len(all_tasks) - len(tasks)
                if skipped_count > 0:
                    print(
                        f"Skipping {skipped_count} tasks from already-completed years: {sorted(completed_years_in_chunk)}"
                    )

                print(
                    f"Generated {len(tasks)} date-range tasks for this chunk (total: {len(all_tasks)})"
                )

                # Create progress bar for task processing
                task_progress = tqdm(
                    total=len(tasks),
                    desc=f"📆 Tasks {chunk_start}→{chunk_end}",
                    unit="task",
                    leave=True,
                    colour="cyan",
                    ncols=100,
                    position=0,
                    file=sys.stderr,
                    dynamic_ncols=True,
                )

                # Process tasks with ThreadPoolExecutor
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as executor:
                    future_to_task = {
                        executor.submit(process_task, task, archive_manager): task
                        for task in tasks
                    }

                    for future in concurrent.futures.as_completed(future_to_task):
                        task = future_to_task[future]

                        # Check timeout during processing
                        if (
                            timeout_seconds
                            and time.time() - start_time >= timeout_seconds
                        ):
                            task_progress.close()
                            task_progress.write(
                                f"⏰ Timeout reached while processing chunk {chunk_start} to {chunk_end}"
                            )
                            task_progress.write(
                                "💾 Cancelling remaining tasks and saving progress..."
                            )

                            # Cancel remaining futures
                            for f in future_to_task:
                                if not f.done():
                                    f.cancel()

                            # Save progress with current state
                            save_fill_progress(
                                overall_start,
                                overall_end,
                                completed_chunks,
                                completed_chunks[-1][1] if completed_chunks else None,
                                years_in_chunk,
                                [chunk_start, chunk_end],
                            )
                            return

                        try:
                            future.result()

                            # Track the year for this task
                            task_year = datetime.strptime(
                                task.from_date, "%Y-%m-%d"
                            ).year

                            # If year changed, upload archives for previous year
                            if current_year is not None and task_year != current_year:
                                task_progress.write(
                                    f"\x1b[36m📤 Year {current_year} → {task_year}, uploading archives...\x1b[0m"
                                )
                                try:
                                    archive_manager.upload_year_archives(current_year)

                                    # Only add to completed years if upload succeeded
                                    years_in_chunk.add(current_year)

                                    # Save progress after each year completes
                                    save_fill_progress(
                                        overall_start,
                                        overall_end,
                                        completed_chunks,
                                        completed_chunks[-1][1]
                                        if completed_chunks
                                        else None,
                                        years_in_chunk,
                                        [chunk_start, chunk_end],
                                    )
                                    task_progress.write(
                                        f"\x1b[32m✅ Year {current_year} upload complete and saved\x1b[0m"
                                    )
                                except Exception as upload_err:
                                    task_progress.write(
                                        f"\x1b[31m❌ Failed to upload year {current_year}: {upload_err}\x1b[0m"
                                    )
                                    # Don't add to years_in_chunk if upload failed

                            current_year = task_year
                            task_progress.update(1)  # Update progress bar

                        except Exception as e:
                            task_progress.write(
                                f"Error processing task {task.from_date} to {task.to_date}: {e}"
                            )
                            import traceback

                            traceback.print_exc()
                            task_progress.update(1)  # Update progress bar even on error

                # Close progress bar
                task_progress.close()

                # Mark loop as completed successfully (before final upload)
                loop_completed_successfully = True

            except KeyboardInterrupt:
                logger.warning("⚠️ Received interrupt signal, saving progress...")
                save_fill_progress(
                    overall_start,
                    overall_end,
                    completed_chunks,
                    completed_chunks[-1][1] if completed_chunks else None,
                    years_in_chunk,
                    [chunk_start, chunk_end],
                )
                raise
            except Exception as e:
                logger.error(f"❌ Error during chunk processing: {e}")
                import traceback

                traceback.print_exc()

            # Upload archives for the last year in chunk
            # ONLY if the loop completed successfully (not interrupted)
            if loop_completed_successfully and current_year is not None:
                print(
                    f"\x1b[36m📤 Uploading archives for final year {current_year} in chunk...\x1b[0m"
                )
                try:
                    archive_manager.upload_year_archives(current_year)

                    # Only add to completed years if upload succeeded
                    years_in_chunk.add(current_year)
                    print(f"\x1b[32m✅ Year {current_year} upload complete\x1b[0m")
                except Exception as upload_err:
                    logger.error(
                        f"\x1b[31m❌ Failed to upload final year {current_year}: {upload_err}\x1b[0m"
                    )
                    # Don't add to years_in_chunk if upload failed

            chunk_changes = archive_manager.get_all_changes()
            upload_metadata = archive_manager.get_upload_metadata()

        # Summarize chunk changes outside the context manager once uploads are done
        if chunk_changes:
            logger.info(f"📊 Chunk {chunk_start} to {chunk_end} summary:")
            logger.info("")
            logger.info("🆕 Change summary for this chunk:")
            for year in sorted(chunk_changes.keys(), key=int):
                logger.info(f"  📁 Year {year}:")
                for archive_type, files in chunk_changes[year].items():
                    logger.info(f"    • {archive_type}: {len(files)} file(s)")
                    # Show preview of files
                    CHANGE_LOG_PREVIEW_LIMIT = 20
                    preview = files[:CHANGE_LOG_PREVIEW_LIMIT]
                    for filename in preview:
                        logger.info(f"       - {filename}")
                    if len(files) > CHANGE_LOG_PREVIEW_LIMIT:
                        logger.info(
                            f"       … plus {len(files) - CHANGE_LOG_PREVIEW_LIMIT} more (see chunk_changes_summary.json)"
                        )
        else:
            logger.info(f"ℹ️  No new files added for chunk {chunk_start} to {chunk_end}")

        summary_payload = {
            "chunk": {"start": chunk_start, "end": chunk_end},
            "generated_at": datetime.now().isoformat(),
            "years": {str(year): meta for year, meta in upload_metadata.items()},
            "files": chunk_changes,
        }

        # Save per-chunk summary (overwrite for current chunk)
        chunk_summary_path = Path("./chunk_changes_summary.json")
        with open(chunk_summary_path, "w") as summary_file:
            json.dump(summary_payload, summary_file, indent=2)
        logger.info(f"📝 Chunk summary written to {chunk_summary_path.resolve()}")

        # Append to cumulative all_changes.json (never deleted)
        all_changes_path = Path("./all_fill_changes.json")
        all_changes = []
        if all_changes_path.exists():
            try:
                with open(all_changes_path, "r") as f:
                    all_changes = json.load(f)
            except Exception:
                all_changes = []

        # Add current chunk to cumulative list
        all_changes.append(summary_payload)

        with open(all_changes_path, "w") as f:
            json.dump(all_changes, f, indent=2)
        logger.info(f"📝 Cumulative changes appended to {all_changes_path.resolve()}")

        # Process metadata to parquet for the years in this chunk
        if years_in_chunk:
            logger.info(
                f"🔄 Processing metadata to parquet for years: {sorted(years_in_chunk)}"
            )
            # Give S3 a moment to propagate the newly uploaded files
            time.sleep(5)
            try:
                # Convert years to strings as expected by SupremeCourtS3Processor
                years_as_strings = [str(year) for year in years_in_chunk]

                processor = SupremeCourtS3Processor(
                    s3_bucket=s3_bucket,
                    s3_prefix=s3_prefix,
                    batch_size=10000,
                    years_to_process=years_as_strings,
                )

                processed_years, total_records = processor.process_bucket_metadata()

                if total_records > 0:
                    logger.info(
                        f"✅ Successfully processed {total_records} records to parquet for {len(processed_years)} years"
                    )
                else:
                    logger.warning("⚠️ No records were processed to parquet")

            except Exception as e:
                logger.error(f"❌ Error processing metadata to parquet: {e}")
                import traceback

                traceback.print_exc()

        # Mark chunk as completed
        completed_chunks.append((chunk_start, chunk_end))
        save_fill_progress(
            overall_start, overall_end, completed_chunks, chunk_end, set(), None
        )

        logger.info(
            f"✅ Completed chunk {len(completed_chunks)}/{len(all_five_year_chunks)}: {chunk_start} to {chunk_end}"
        )

        # Check timeout after completing a chunk
        if timeout_seconds and time.time() - start_time >= timeout_seconds:
            logger.warning(
                f"⏰ Timeout reached after completing chunk {chunk_start} to {chunk_end}"
            )
            logger.info("💾 Progress saved. Run again to continue from next chunk.")
            remaining = len(all_five_year_chunks) - len(completed_chunks)
            logger.info("")
            logger.info("📌 " + "=" * 66)
            logger.info(f"📌 Chunk completed! {remaining} chunks remaining")
            logger.info("📌 Run the same command again to continue processing")
            logger.info("📌 " + "=" * 66)
            return

        # Continue to next chunk in the loop

    # All chunks completed (loop finished naturally)
    logger.info("")
    logger.info("🎉 " + "=" * 66)
    logger.info("🎉 ALL CHUNKS COMPLETED! Gap-filling process finished successfully!")
    logger.info("🎉 " + "=" * 66)
    clear_fill_progress()
