eToolBox: A set of tools and functions we use across projects
=======================================================================================

.. readme-intro

.. image:: https://github.com/rmi/etoolbox/workflows/tox-pytest/badge.svg
   :target: https://github.com/rmi/etoolbox/actions?query=workflow%3Atox-pytest
   :alt: Tox-PyTest Status

.. image:: https://github.com/rmi/etoolbox/workflows/docs/badge.svg
   :target: https://rmi.github.io/etoolbox/
   :alt: GitHub Pages Status

.. image:: https://coveralls.io/repos/github/RMI/etoolbox/badge.svg?branch=main
   :target: https://coveralls.io/github/RMI/etoolbox?branch=main

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black>
   :alt: Any color you want, so long as it's black.

.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json
    :target: https://github.com/astral-sh/ruff
    :alt: Ruff

Desciption
=======================================================================================

*  datazip

   *  `DataZip <https://rmi.github.io/etoolbox/autoapi/etoolbox/datazip/core/index.html#etoolbox.datazip.core.DataZip>`_
      - an extension of
      `ZipFile <https://docs.python.org/3/library/zipfile.html#zipfile-objects>`_ with
      a couple useful features:

      *  Support for easily storing and retrieving a range of Python objects, including
         builtins, pandas and numpy objects, and certain custom objects.
      *  Store dataframes as parquets for space efficiency.

   *  IOMixin - a mixin that allows a class to be stored in a DataZip. Usually.

*  utils

   * arrays - pandas and numpy comparison aids.
   * match - helpers for Python's ``match`` syntax.
   * pudl - tools for reading data from PUDL

Installation
=======================================================================================

Dispatch can be installed and used in it's own environment or installed into another
environment using pip. To install it using pip:

.. code-block:: bash

   pip install git+https://github.com/rmi/etoolbox.git

Or from the dev branch:

.. code-block:: bash

   pip install git+https://github.com/rmi/etoolbox.git@dev


.. warning::

   Version 0.2.0 is the last version that supports the legacy PUDL objects such as
   ``PudlTable`` and contains tools for replicating and working with those objects. You
   can install a specific version like this:

   .. code-block:: bash

      pip install git+https://github.com/rmi/etoolbox.git@0.2.0


To create an environment for eToolbox, navigate to the repo folder in terminal and run:

.. code-block:: bash

   mamba update mamba
   mamba env create --name etb --file environment.yml

If you get a ``CondaValueError`` that the prefix already exists, that means an
environment with the same name already exists. You must remove the old one before
creating the new one:

.. code-block:: bash

   mamba update mamba
   mamba env remove --name etb
   mamba env create --name etb --file environment.yml


PUDL Data Access
=======================================================================================
Setup
---------------------------------------------------------------------------------------
No setup is necessary beyond having the ``etoolbox`` library installed in your
environment.

Usage
---------------------------------------------------------------------------------------
Any table that is in the ``pudl.sqlite`` can be read using these functions without
needing to download the entire database.

.. code-block:: python

   from etoolbox.utils.pudl import pd_read_pudl

   df = pd_read_pudl("core_eia__codes_balancing_authorities")


.. note::

   ``pd_read_pudl`` and its polars siblings will use the ``nightly`` release by default.
   For any work where reproducibility is useful (i.e. almost everywhere), you are
   **highly** encouraged to use a versioned data release. You can find available releases
   with the ``pudl_list`` function.

   .. code-block:: python

      from etoolbox.utils.pudl import pudl_list

      pudl_list(None)

   And then define the release as below. It's useful to set it as a global variable that
   can be used anytime PUDL data is loaded for consistency.

   .. code-block:: python

      PUDL_RELEASE = "vYYYY.MM.DD"

      df = pd_read_pudl("core_eia__codes_balancing_authorities", release=PUDL_RELEASE)


More information about the tables are available in
`this data dictionary <https://catalystcoop-pudl.readthedocs.io/en/nightly/data_dictionaries/pudl_db.html#pudl-data-dictionary>`_.
New and old names for the tables are available
`here <https://docs.google.com/spreadsheets/d/1RBuKl_xKzRSLgRM7GIZbc5zUYieWFE20cXumWuv5njo/edit#gid=1126117325>`_.

.. warning::

   If you use PyCharm and get a ``TypeError`` when using these functions in the
   debugger, you may need to change PyCharm settings, see
   `PY-71488 <https://youtrack.jetbrains.com/issue/PY-71488>`_ for more information.


PUDL in tests
---------------------------------------------------------------------------------------
By default, any tests that you run locally will use the same cached PUDL data that you
use when you run your code normally. If you want tests to always run as if no cache
existed, the following code examples create a temporary cache folder which is used by
your tests and then deleted.

   conftest.py

   .. code-block:: python

      from etoolbox.utils.pudl import rmi_pudl_init

      @pytest.fixture(scope="session")
      def temp_dir() -> Path:
          """Return the path to a temp directory that gets deleted on teardown."""
          out = Path(__file__).parent / "temp"
          if out.exists():
              shutil.rmtree(out)
          out.mkdir(exist_ok=True)
          yield out
          shutil.rmtree(out)


      @pytest.fixture(scope="session")
      def pudl_test_cache(temp_dir):  # noqa: PT004
          """Change PUDL cache path for testing."""
          import etoolbox.utils.pudl as pudl

          pudl.CACHE_PATH = temp_dir / "pudl_cache"


   pudl_access_test.py

   .. code-block:: python

      from etoolbox.utils.pudl import pd_read_pudl


      @pytest.mark.usefixtures("pudl_test_cache")
      def test_pd_read_pudl_table():
         """Test reading table from GCS as :func:`pandas.DataFrame."""
         df = pd_read_pudl("core_eia__codes_balancing_authorities")
         assert not df.empty
