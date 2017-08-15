# acsql
The Advanced Camera for Surveys Quicklook Project

Official Documentation: http://acsql.readthedocs.io/

## Installation

#### Dependencies

The `acsql` package requires the following dependencies:

- `python 3.+`
- `astropy`
- `flask`
- `numpy`
- `numpydoc`
- `pymysql`
- `requests`
- `sqlalchemy`
- `wtforms`
- `wtforms_components`
- `stak` (http://github.com/spacetelescope/stak)

#### Installing the `acsql` Package

The `acsql` application can be installed by cloning this repository and running the `setup.py` script:

```
git clone https://github.com/spacetelescope/acsql.git
cd acsql
python setup.py install
```

Users must fill in the `config.yaml` configuration file, located at `acsql/utils/config.yaml`:

```yaml
connection_string : 'mysql+pymysql://username:password@host:12345/database'
filesystem : ''
log_dir : ''
jpeg_dir : ''
thumbnail_dir : ''
ncores : 1
```

The `connection_string` item should contain the users credentials to the `acsql` database.  Please ask [@bourque](http://github.com/bourque) to set up an account.

The `filesystem` item should point to the directory that holds ACS FITS files in a directory structure of
`<proposal_id>/<rootname>/<rootname_<filetype>.fits`, for example:

```
filesystem : '/Users/bourque/filesystem/'
```

where `/Users/bourque/filesystem/` contains subdirectories such as:

```
/users/bourque/filesystem/jbm1/
/users/bourque/filesystem/jbm1/jbm110u2q/
/users/bourque/filesystem/jbm1/jbm110u0q/
/users/bourque/filesystem/jbm1/jbm110tyq/
/users/bourque/filesystem/jbm1/jbm110txq/
/users/bourque/filesystem/jbm1/jbm110010/
/users/bourque/filesystem/jbm1/jbm105y7q/
/users/bourque/filesystem/jbm1/jbm105y5q/
/users/bourque/filesystem/jbm1/jbm105y3q/
/users/bourque/filesystem/jbm1/jbm105y2q/
/users/bourque/filesystem/jbm1/jbm105010/
```

and each one of these subdirectories contains the data files, for example:

```
>>> ls /users/bourque/filesystem/jbm1/jbm110u2q/

jbm110u2q_raw.jpg
jbm110u2q_flt.jpg
jbm110u2q_trl.fits
jbm110u2q_flt_hlet.fits
jbm110u2q_spt.fits
jbm110u2q_flt.fits
jbm110u2q_flc.fits
jbm110u2q_raw.fits
```

In production, this `filesystem` item points to the directory of the MAST Online Cache.

The `log_dir` item should point to a directory in which log files that describe code execution will be written.

The `jpeg_dir` item should point to a directory in which JPEGs will be written.

The `thumbnail_dir` item should point to a directory in which smaller Thumbnail images will be written.

The `ncores` item is set to the number of processors that should be used when performing data ingestion.

#### Running the `acsql` web application locally:

Once the `acsql` package is installed, the `acsql` web application can be run locally:

```
python acsql/website/acsql_webapp.py
```

The just go to `localhost:5000` in a browser.
