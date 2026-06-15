"""Shared H3 resolution constant.

Kept dependency-free (no pyspark) so the FastAPI/Streamlit serving layer can
use it without pulling in a Spark/JVM dependency.
"""

from __future__ import annotations

# Resolution 9 cells are ~0.1 km^2, roughly city-block sized -- fine grained
# enough to distinguish individual Citi Bike stations without exploding the
# number of distinct cells.
H3_RESOLUTION = 9
