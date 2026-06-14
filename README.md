# Stream-Processing-Assignment-Eng-5785
A PySpark Structured Streaming pipeline that monitors IoMT patient data to detect sustained abnormal heart rates using tumbling time windows.

Why chose this window type (Tumbling 2 min)? Each time block is 2 minutes long and non-overlapping, and is called a "tumbling window" (e.g., 10:00-10:02, 10:02-10:04). This is the perfect way to measure health outcomes over time since each record is only included in one assessment period. With a sliding window, a single 1 minute spike could "bleed through" to several windows, causing the "sustained" alarm to be triggered.

So, where does your pipeline need state? The State must be shown in two separate locations in this pipeline:

The Aggregation: Spark Structured Streaming also keeps internal state memory to buffer incoming patient records and continuously compute the running average heart rate for the current 2-minute window until the watermark gives permission for the window to safely close.
In this case, the pipeline needs to have external state or a batch-to-batch state that is maintained across the pipeline (driver logic in foreachBatch in this assignment), to "remember" which patients had an elevated heart rate in the previous window.
