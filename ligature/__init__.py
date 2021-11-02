"""
    Stitch data together

    RecordSets form the core of Ligature. A list of tuples of tuples,
      this provides a framework for chunky updates in a (mostly) immutable
      data structure.

    Scanners iterate RecordSets and provide iterators that scan RecordSets
      in a column-like way while still maintaining row-based chunks.

    Transforms and Composables allow Scanners to be mapped to RecordSets
      and perform generator-like updates, creating new RecordSets and
      making chains of calculations possible. These are evaluated lazily
      by default, allowing data to be updated and computed in asynchronous
      chunks and resolved on demand (if desired).
"""


__version__ = '1.5.0'