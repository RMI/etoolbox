=======================================================================================
eToolBox and R
=======================================================================================

.. _etb-r-label:

With a little setup, the functions provided in eToolBox can be used from
R. These functions provide easy access to PUDL data, including with a local cache, so
data only need be downloaded once. They also provide a similar functionality for data
stored in Azure.

These setup instructions are written for and tested on macOS. They likely will work
with little if any modification on Linux. On Windows, all bets are off, you'll likely
need to check out the
`uv install <https://docs.astral.sh/uv/getting-started/installation/>`__ or
`miniforge install <https://github.com/conda-forge/miniforge#windows>`__  instructions.
If you do get it working let us know how you did it!

Setup eToolBox with UV
--------------------------------------------------------------------------------------

Install `uv <https://github.com/astral-sh/uv>`__ with its standalone installer.

.. code-block:: zsh

   curl -LsSf https://astral.sh/uv/install.sh | sh

**Or** Install `uv <https://github.com/astral-sh/uv>`__ with Homebrew.

.. code-block:: zsh

   brew install uv

Setup uv for eToolBox, install it and store the SAS key to enable
access to data stored in Azure.

.. code-block:: zsh

    uv python install 3.13
    uv tool install git+https://github.com/rmi/etoolbox.git --compile-bytecode
    uv tool update-shell
    etb cloud init "<token>"

If you find that the ``etb`` command does not work even after restarting the terminal,
you will need to make a manual change to ``.zshrc``.

.. code-block:: zsh

    echo -n 'export PATH=~/.local/bin:$PATH' >> ~/.zshrc

To use eToolBox in R you must install the R package
`reticulate <https://rstudio.github.io/reticulate/>`__. Then you need to tell
it where to find python using ``use_python``. The examples below show what this looks
like on a macOS with Python installed using ``uv``.

Using eToolBox to read and write patio result data to Azure
===========================================================
Details about available functions for use with Azure can be found in the API reference
:mod:`.cloud`.

.. code-block:: R

    library(reticulate)

    use_python("~/.local/share/uv/tools/rmi-etoolbox/bin/python")

    # import pudl module from etoolbox
    cloud <- import("etoolbox.utils.cloud")

    # read patio resource model results
    results <- cloud$read_patio_resource_results("202504270143")

    # write ``result_df`` to ``output_file`` parquet in the ``202504270143``
    # model run directory on Azure
    cloud$write_patio_econ_results(result_df, "202504270143", "output_file.parquet")

    # list all available results on Azure
    cloud$cloud_list("patio-results")

Using eToolBox to read PUDL data
================================
Details about available functions for use with PUDL can be found in the API reference
:mod:`.pudl`.

.. code-block:: R

    library(reticulate)

    use_python("~/.local/share/uv/tools/rmi-etoolbox/bin/python")

    # to get consistent PUDL data and for reproducibility, set the pudl_release globally
    pudl_release <- "v2025.2.0"

    # import pudl module from etoolbox
    pudl <- import("etoolbox.utils.pudl")

    # read a pudl table
    df <- pudl$pd_read_pudl("out_eia__yearly_utilities", release=pudl_release)

    # list all pudl releases
    pudl$pudl_list(NULL)

    # list pudl tables in ``pudl_release`` release
    pudl$pudl_list(pudl_release)


Setup eToolBox with Miniforge
--------------------------------------------------------------------------------------

Current recommendation is to use ``uv`` as described above. However if you already use
mamba or conda, or wDownload `miniforge <https://github.com/conda-forge/miniforge>`__
and install it. It may ask about adding mamba to your path or about initializing mamba.
Unless you have a reason to say no, you'll want to say yes.

.. code-block:: zsh

   curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
   bash Miniforge3-$(uname)-$(uname -m).sh

Create a conda environment for eToolBox, install it and store the SAS key to enable
access to data stored in Azure.

.. code-block:: zsh

    mamba create -n etb python=3.13 pip
    mamba activate etb
    pip install git+https://github.com/rmi/etoolbox.git
    mamba activate etb
    etb cloud init "<token>"

To use eToolBox in R you must install the R package
`reticulate <https://rstudio.github.io/reticulate/>`__. Then you need to tell
it where to find python using ``use_condaenv``. You may need to look in your home
directory to see what the ``miniforge`` directory is called, it should be
``miniforge``, ``miniforge3`` or something like that, you then use that instead of
``<miniforge>`` in the samples below.

.. note::

    The reticulate documentation describes other ways of setting up and configuring the
    python side of this. So long as eToolBox is installed on the path of a python
    interpreter and you can point reticulate at that interpreter, this should work.

Using eToolBox to read and write patio result data to Azure
===========================================================
Details about available functions for use with Azure can be found in the API reference
:mod:`.cloud`.

.. code-block:: R

    library(reticulate)

    use_condaenv("~/<miniforge>/envs/etb")

    # import pudl module from etoolbox
    cloud <- import("etoolbox.utils.cloud")

    # read patio resource model results
    results <- cloud$read_patio_resource_results("202504270143")

    # write ``result_df`` to ``output_file`` parquet in the ``202504270143``
    # model run directory on Azure
    cloud$write_patio_econ_results(result_df, "202504270143", "output_file.parquet")

    # list all available results on Azure
    cloud$cloud_list("patio-results")

Using eToolBox to read PUDL data
================================
Details about available functions for use with PUDL can be found in the API reference
:mod:`.pudl`.

.. code-block:: R

    library(reticulate)

    use_condaenv("~/<miniforge>/envs/etb")

    # to get consistent PUDL data and for reproducibility, set the pudl_release globally
    pudl_release <- "v2025.2.0"

    # import pudl module from etoolbox
    pudl <- import("etoolbox.utils.pudl")

    # read a pudl table
    df <- pudl$pd_read_pudl("out_eia__yearly_utilities", release=pudl_release)

    # list all pudl releases
    pudl$pudl_list(NULL)

    # list pudl tables in ``pudl_release`` release
    pudl$pudl_list(pudl_release)
