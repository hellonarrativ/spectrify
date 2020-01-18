=======
History
=======

3.1.0 (2020-01-18)
------------------

* Remove psycopg2 requirement (allows use of either psycopg2 or psycopg2-binary)


3.0.1 (2019-11-26)
------------------

* Fix changelog


3.0.0 (2019-11-26)
------------------
Backwards incompatible changes:

* Add REGION parameter to UNLOAD operations
* Bugfix: Correctly construct path for S3 bucket in "create-table" command

Other Changes:

* Support for obtaining credentials with AWS session token
* Upgrade to pytest v4.6.6
* Fix Flake8 errors


2.0.0 (2019-03-09)
------------------

* Default to 256MB files
* Flag for unicode support on Python 2.7 (performance implications)
* Drop support for Python 3.4
* Support for additional CSV format parameters
* Support for REAL data type


1.0.1 (2018-07-12)
------------------

* Loosen version requirement for PyArrow
* Add example script
* Update documentation


1.0.0 (2018-04-20)
------------------

* Move functionality into classes to make customizing behavior easier
* Add support for DATE columns
* Add support for DECIMAL/NUMERIC columns
* Upgrade to pyarrow v0.9.0


0.4.1 (2018-03-25)
------------------

* Fix exception when source table is not in schema public


0.4.0 (2018-02-25)
------------------

* Upgrade to pyarrow v0.8.0
* Verify Redshift column types are supported before attempting conversion
* Bugfix: Properly clean up multiprocessing.pool resource


0.3.0 (2017-10-30)
------------------

* Support 16- and 32-bit integers
* Packaging updates


0.2.1 (2017-09-27)
------------------

* Fix Readme


0.2.0 (2017-09-27)
------------------

* First release on PyPI.


0.1.0 (2017-09-13)
------------------

* Didn't even make it to PyPI.
