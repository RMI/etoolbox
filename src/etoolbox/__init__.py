"""A template repository for a Python package.

Created by Catalyst Cooperative, modified by RMI.
"""
import logging
from importlib.metadata import PackageNotFoundError, version

# In order for the package modules to be available when you import the package,
# they need to be imported here somehow. Not sure if this is best practice though.

# Create a root logger for use anywhere within the package.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__author__ = "RMI"
__contact__ = "...@rmi.org"
__maintainer__ = "Cheshire Cat"
__license__ = "BSD 3-Clause License"
__maintainer_email__ = "...@rmi.org"
__docformat__ = "restructuredtext en"
__description__ = "A template for Python package repositories."
__long_description__ = """
This should be a paragraph long description of what the package does.
"""

try:
    from etoolbox._version import version as __version__
except ImportError:
    logger.warning("Version unknown because package is not installed.")
    __version__ = "unknown"

__projecturl__ = "https://github.com/rmi-electricity/etoolbox"
__downloadurl__ = "https://github.com/rmi-electricity/etoolbox"
