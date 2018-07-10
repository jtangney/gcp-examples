# Dataflow Files
Simple Dataflow Pipeline demonstrating how to process individual files as single PCollection elements.

Use case is when pipeline inputs are entire files; the information for a
single 'record' is split over many lines within a single file, rather than
each individual line within the file representing a record (as with TextIO).
