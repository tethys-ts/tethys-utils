lsrm
==================================

This git repository contains project code for the land surface recharge models (lsrm) developed at Environment Canterbury (ECan).
The current lsrm available was originally developed by David Scott in 2013 in Fortran, but has been transcribed and improved by Mike Kittridge into this Python package.

Installation
------------
Conda is your best bet for installing lsrm. You'll also need python 3.6 and geopandas 0.5.1.

.. code::

  conda install -c mullenkamp lsrm geopandas=0.5.1

ds module
----------
The ds module contains the LSRM class for running the transcribed David Scott lsrm.

Initialisation does not require any parameters:

.. code::

  from lsrm.ds import LSRM

  l1 = LSRM()

Then follow the sequence of methods: soils_import, input_processing, and lsrm.

.. code::

  irr1, psw1 = l1.soils_import()

  mv, sites = l1.input_processing(bound_shp, grid_res, buffer_dis, interp_fun, agg_ts_fun, time_agg)

  results1 = l1.lsrm()

Documentation on the David Scott's lsrm methods can be found `here <https://github.com/Data-to-Knowledge/lsrm/raw/master/sphinx/source/docs/David_Scott_lsrm_2013.pdf>`_
