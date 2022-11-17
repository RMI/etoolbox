=======================================================================================
eToolBox Release Notes
=======================================================================================

.. _release-v0-1-0:

---------------------------------------------------------------------------------------
0.1.0 (2022-XX-XX)
---------------------------------------------------------------------------------------

What's New?
^^^^^^^^^^^
*  Migrating :class:`.DataZip` from
   `rmi.dispatch <https://github.com/rmi-electricity/dispatch>`_ where it didn't really
   belong. Also added additional functionality including recursive writing and reading
   of :class:`list`, :class:`dict`, and :class:`tuple` objects.
*  Created :class:`.IOMixin` and :class:`IOWrapper` to make it easier to add
   :class:`.DataZip` to other classes.
*  Migrating :func:`.compare_dfs` from the Hub.
*  Updates to :class:`.DataZip`, :class:`.IOMixin`, and :class:`IOWrapper` to better
   better manage attributes missing from original object or file representation of
   object. Including ability to use differently organized versions of
   :class:`.DataZip`.
*  Clean up of :class:`.DataZip` internals, both within the object and in laying out
   files. Particularly of how metadata and attributes are stored. Added
   :meth:`.DataZip.readm` and :meth:`.DataZip.writem` to read and write additional
   metadata not core to :class:`.DataZip`.
*  Added support for storing :class:`numpy.array` objects in :class:`.DataZip` using
   :func:`numpy.load` and :func:`numpy.save`.

Known Issues
^^^^^^^^^^^^
*  Recipe system is fragile and bespoke, there really should be a better way...
