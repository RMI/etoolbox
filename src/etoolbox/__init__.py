"""A range of tools and helpers for the RMI electricity team."""

import logging

# Create a root logger for use anywhere within the package.
logger = logging.getLogger("etoolbox")

__author__ = "RMI"
__contact__ = "aengel@rmi.org"
__maintainer__ = "Alex Engel"
__license__ = "BSD 3-Clause License"
__maintainer_email__ = "aengel@rmi.org"
__docformat__ = "restructuredtext en"
__description__ = "A range of tools and helpers for the RMI electricity team."

try:
    from etoolbox._version import version as __version__
except ImportError:
    logger.warning("Version unknown because package is not installed.")
    __version__ = "unknown"

__projecturl__ = "https://github.com/rmi-electricity/etoolbox"
__downloadurl__ = "https://github.com/rmi-electricity/etoolbox"
