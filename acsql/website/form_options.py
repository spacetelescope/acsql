"""Provides a dictionary containing ``acsql`` database query form data
for use by the ``acsql`` web application.

Authors
-------

    - Matthew Bourque
    - Meredith Durbin

Use
---

    The dictionary contained in this module is inteded to be imported
    as such:
    ::

        from acsql.website.form_options import FORM_OPTIONS
"""

APERTURES = ['WFC', 'WFC-FIX', 'WFC1', 'WFC1-1K', 'WFC1-2K', 'WFC1-512',
    'WFC1-CTE', 'WFC1-FIX', 'WFC1-IRAMP', 'WFC1-IRAMPQ', 'WFC1-MRAMP',
    'WFC1-MRAMPQ', 'WFC1-POL0UV', 'WFC1-POL0V', 'WFC1-POL120UV', 'WFC1-POL120V',
    'WFC1-POL60UV', 'WFC1-POL60V', 'WFC1A-1K', 'WFC1A-2K', 'WFC1A-512', 'WFC1B-1K',
    'WFC1B-2K', 'WFC1B-512', 'WFC2', 'WFC2-2K', 'WFC2-FIX', 'WFC2-MRAMP',
    'WFC2-ORAMP', 'WFC2-ORAMPQ', 'WFC2C-1K', 'WFC2C-2K', 'WFC2C-512', 'WFC2D-1K',
    'WFC2D-2K', 'WFC2D-512', 'WFCENTER', 'HRC', 'HRC-512', 'HRC-ACQ', 'HRC-CORON1.8',
    'HRC-CORON3.0', 'HRC-FIX', 'HRC-OCCULT0.8', 'HRC-SUB1.8', 'SBC', 'SBC-FIX']
DETECTORS = ['WFC','HRC','SBC']
FILTER1S = ['F115LP', 'F122M', 'F125LP', 'F140LP', 'F150LP', 'F165LP',
    'F475W', 'F502N', 'F550M', 'F555W', 'F606W', 'F625W', 'F658N', 'F775W',
    'F850LP', 'F892N', 'G800L', 'POL0UV', 'POL60UV', 'POL120UV' 'PR110L',
    'PR130L', 'CLEAR1L', 'CLEAR1S', 'BLOCK1', 'BLOCK3', 'BLOCK4', 'NotCmded']
FILTER2S = ['F220W', 'F250W', 'F330W', 'F344N', 'F435W', 'F660N', 'F814W',
    'FR388N', 'FR423N', 'FR462N', 'FR459M', 'FR505N', 'FR551N', 'FR601N',
    'FR647M', 'FR656N', 'FR716N', 'FR782N', 'FR853N', 'FR914M', 'FR931N',
    'FR1016N', 'POL0V', 'POL60V', 'POL120V', 'PR200L', 'CLEAR2L', 'CLEAR2S',
    'NotCmded']
IMAGETYPS = ['BIAS', 'DARK', 'FLAT', 'EXT']
OBSTYPES = ['IMAGING', 'SPECTROSCOPC', 'CORONOGRAPHIC', 'INTERNAL']
OUTPUT_COLUMNS = [('rootname','Rootname'), ('detector','Detector'),
    ('proposal_type','Proposal Type'), ('pr_inv_l','PI Last Name'),
    ('pr_inv_f','PI First Name'), ('proposid','Proposal ID'), ('filter1','Filter1'),
    ('filter2','Filter2'), ('aperture','Aperture'), ('expstart','Expstart'),
    ('date-obs','Date of Observation'), ('time-obs','Time of Observation'),
    ('targname','Target Name'), ('ra_targ','Target RA'), ('dec_targ','Target Dec'),
    ('obstype','Observation Type'), ('obsmode','Observation Mode'),
    ('subarray','Subarray'), ('imagetyp', 'Image Type'), ('asn_id','Association ID')]
OUTPUT_FORMAT = [('table','HTML table'), ('csv','CSV'), ('thumbnails','Thumbnails')]
PROPOSAL_TYPES = ['GO', 'GTO/ACS', 'CAL/ACS', 'SM3/ACS', 'SM3/ERO', 'SNAP',
    'GO/PAR', 'GO/DD', 'GTO/COS', 'CAL/OTA', 'ENG/ACS', 'NASA', 'SM4/ACS',
    'SM4/ERO', 'SM4/COS', 'CAL/WFC3', 'CAL/STIS']


FORM_OPTIONS = {}
FORM_OPTIONS['aperture'] = [(aperture, aperture) for aperture in APERTURES]
FORM_OPTIONS['detector'] = [(detector, detector) for detector in DETECTORS]
FORM_OPTIONS['filter1'] = [(filter1, filter1) for filter1 in FILTER1S]
FORM_OPTIONS['filter2'] = [(filter2, filter2) for filter2 in FILTER2S]
FORM_OPTIONS['imagetyp'] = [(imagetyp, imagetyp) for imagetyp in IMAGETYPS]
FORM_OPTIONS['obstype'] = [(obstype, obstype) for obstype in OBSTYPES]
FORM_OPTIONS['output_columns'] = OUTPUT_COLUMNS
FORM_OPTIONS['output_format'] = OUTPUT_FORMAT
FORM_OPTIONS['proposal_type'] = [(prop_type, prop_type) for prop_type in PROPOSAL_TYPES]
