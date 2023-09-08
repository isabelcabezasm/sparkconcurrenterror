import os
import sys

"""Top-level package for sample."""

__version__ = 0.1

# recognize c4m_common when building as dist
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
