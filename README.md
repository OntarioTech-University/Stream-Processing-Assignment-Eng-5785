# Stream-Processing-Assignment-Eng-5785
Real-Time Stream Processing: Hospital Patient Monitoring

A PySpark Structured Streaming pipeline that monitors IoMT patient data to detect sustained abnormal heart rates using tumbling time windows.

This repository contains a Spark Structured Streaming application built for the ENGR 5785G Real-time Data Analytics for IoT course. It processes simulated IoMT streams to detect sustained abnormal heart rates in ICU patients. 

## Pipeline Architecture
- **Windowing Strategy:** Tumbling 2-Minute Window.
- **Watermarking:** 1-Minute late data tolerance.
- **Alert Condition:** Evaluates whether a patient's average heart rate exceeds 100 bpm across two consecutive windows.

## Project Structure
- `hospital_monitor.ipynb`: The complete, single-cell PySpark streaming job designed for Google Colab/Jupyter environments. It includes the schema definition, window aggregation, state management, and an automated data simulator.

## How to Run the Project (Google Colab)
1. Open a new [Google Colab Notebook](https://colab.research.google.com/).
2. Run `!pip install pyspark` in the first cell to install the required dependencies.
3. Copy the entire contents of the PySpark script into a new cell.
4. Run the cell. 

**What happens when you run it:**
- The script automatically provisions a local `data_stream` directory and clears any old checkpoints.
- The continuous streaming job starts monitoring the directory.
- The script automatically writes 3 consecutive micro-batches (CSV files) into the directory to simulate a live data feed.
- The `processAllAvailable()` command forces the environment to wait until the stream is finalized.
- A clinical alert is written to `alerts.log` and printed directly to the console for Patient P001.

Why chose this window type (Tumbling 2 min)? Each time block is 2 minutes long and non-overlapping, and is called a "tumbling window" (e.g., 10:00-10:02, 10:02-10:04). This is the perfect way to measure health outcomes over time since each record is only included in one assessment period. With a sliding window, a single 1 minute spike could "bleed through" to several windows, causing the "sustained" alarm to be triggered.

So, where does your pipeline need state? The State must be shown in two separate locations in this pipeline:

The Aggregation: Spark Structured Streaming also keeps internal state memory to buffer incoming patient records and continuously compute the running average heart rate for the current 2-minute window until the watermark gives permission for the window to safely close.
In this case, the pipeline needs to have external state or a batch-to-batch state that is maintained across the pipeline (driver logic in foreachBatch in this assignment), to "remember" which patients had an elevated heart rate in the previous window.
