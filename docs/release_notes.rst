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


Known Issues
^^^^^^^^^^^^
*  Recipe system is fragile and bespoke, there really should be a better way...
