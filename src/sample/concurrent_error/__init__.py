# -*- coding: utf-8 -*-

"""Top-level package for osr_processing."""

import os
import sys

__version__ = 0.1

# recognize c4m_common when building as dist
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
