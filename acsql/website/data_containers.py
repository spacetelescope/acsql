"""Various functions for creating and returning various data to be used
by the ``acsql`` web application.

This module contains functions to obtains image and proposal metadata
for use by the ``acsql`` web application.  See the ``acsql_webapp``
module for further information about the web application.

Authors
-------

    - Matthew Bourque
    - Meredith Durbin

Use
---

    This module is intended to be imported and used by ``acsql_webapp``
    as such:
    ::

        from acsql.website.data_containers import get_view_image_dict
        from acsql.website.data_containers import get_view_proposal_dict

        image_dict = get_view_image_dict(proposal, filename, fits_type)
        proposal_dict = get_view_proposal_dict(proposal)

Dependencies
------------

    - ``acsql``
"""


import glob
import html
import os
import requests

from acsql.database import database_interface
from acsql.database.database_interface import Master
from acsql.utils.utils import SETTINGS


def _get_image_lists(data_dict, fits_type):
    """Add a list of JPEG and Thumbnail paths to the ``data_dict``
    dictionary.

    Parameters
    ----------
    data_dict : dict
        A dictionary containing data used to render a webpage.
    fits_type : str
        The FITS type.  Can either be ``raw``, ``flt``, or ``flc``.

    Returns
    -------
    data_dict : dict
        A dictionary containing data used to render a webpage.
    """

    jpeg_proposal_path = os.path.join('static/img/jpegs/', data_dict['proposal_id'])
    thumb_proposal_path = os.path.join('static/img/thumbnails/', data_dict['proposal_id'])

    data_dict['jpegs'] = sorted(glob.glob(os.path.join(jpeg_proposal_path, '*flt.jpg')))
    data_dict['thumbs'] = sorted(glob.glob(os.path.join(thumb_proposal_path, '*flt.thumb')))

    return data_dict


def _get_metadata_from_database(data_dict):
    """Add observation metadata (e.g. ``aperture``, ``exptime``, etc.)
    to the ``data_dict`` by querying the ``acsql`` database.

    Parameters
    ----------
    data_dict : dict
        A dictionary containing data used to render a webpage.

    Returns
    -------
    data_dict : dict
        A dictionary containing data used to render a webpage.
    """

    session = getattr(database_interface, 'session')

    results = []
    for rootname in data_dict['rootnames']:

        detector = session.query(Master.detector)\
            .filter(Master.rootname == rootname).one()[0]

        table = getattr(database_interface, '{}_raw_0'.format(detector))
        result = session.query(
            table.aperture, table.exptime, table.filter1, table.filter2,
            table.targname, getattr(table, 'date-obs'),
            getattr(table, 'time-obs'), table.expstart, table.expflag,
            table.quality, table.ra_targ, table.dec_targ, table.pr_inv_f,
            table.pr_inv_l).filter(table.rootname == rootname).one()
        result = [item for item in result]
        result.append(detector)
        results.append(result)

    session.close()

    # Parse the results
    data_dict['detectors'] = [detector for item in results]
    data_dict['apertures'] = [item[0] for item in results]
    data_dict['exptimes'] = [item[1] for item in results]
    data_dict['filter1s'] = [item[2] for item in results]
    data_dict['filter2s'] = [item[3] for item in results]
    data_dict['targnames'] = [item[4] for item in results]
    data_dict['dateobss'] = [item[5] for item in results]
    data_dict['timeobss'] = [item[6] for item in results]
    data_dict['expstarts'] = [item[7] for item in results]
    data_dict['expflags'] = [item[8] for item in results]
    data_dict['qualitys'] = [item[9] for item in results]
    data_dict['ras'] = [item[10] for item in results]
    data_dict['decs'] = [item[11] for item in results]
    data_dict['pi_firsts'] = [item[12] for item in results]
    data_dict['pi_lasts'] = [item[13] for item in results]

    return data_dict


def _get_buttons_dict(data_dict):
    """Add data used for various buttons on the ``/archive/<proposal>``
    page or ``/database/results/`` page to the ``data_dict``.

    Parameters
    ----------
    data_dict : dict
        A dictionary containing data for the given
        ``/archive/<proposal>`` or ``/database/results/`` page, such as
        ``visits`` and ``targnames``

    Returns
    -------
    data_dict : dict
        A dictionary containing data for the given
        ``/archive/<proposal>`` or ``/database/results/`` page, such as
        ``visits`` and ``targnames``
    """

    data_dict['buttons'] = {}
    data_dict['buttons']['detector'] = sorted(set(data_dict['detectors']))
    data_dict['buttons']['visit'] = sorted(set(data_dict['visits']))
    data_dict['buttons']['target'] = sorted(set(data_dict['targnames']))
    data_dict['buttons']['filter'] = sorted(set([
        '{}/{}'.format(filter1, filter2)
        for filter1, filter2
        in zip(data_dict['filter1s'], data_dict['filter2s'])]))

    return data_dict


def _get_proposal_status(data_dict):
    """Add proposal status information (e.g. ``proposal_title``,
    ``cycle``, etc.) to the ``data_dict``.

    The proposal status information is scraped from the proposal
    status webpage.

    Parameters
    ----------
    data_dict : dict
        A dictionary containing data used to render a webpage.

    Returns
    -------
    data_dict : dict
        A dictionary containing data used to render a webpage.
    """

    data_dict['status_page'] = (
        'http://www.stsci.edu/cgi-bin/get-proposal-info?id={}'
        '&submit=Go&observatory=HST').format(data_dict['proposal_id'])

    req = requests.get(data_dict['status_page'], timeout=3)

    if req.ok:

        status_string = req.content.decode()
        data_dict['proposal_title'] = html.unescape(status_string.\
            split('<b>Title:</b> ')[1].\
            split('<br>')[0])
        data_dict['cycle'] = html.unescape(status_string.\
            split('<b>Cycle:</b> ')[1].\
            split('<br>')[0])
        data_dict['schedule'] = html.unescape(status_string.\
            split('/proposal-help-HST.html#')[2].\
            split('">')[0])

    else:
        print('Request failed: {}'.format(data_dict['status_page']))
        data_dict['proposal_title'] = 'proposal title unavailable'
        data_dict['cycle'] = None
        data_dict['schedule'] = None

    return data_dict


def _initialize_data_dict(proposal, fits_type='flt'):
    """Create and return a dictionary containing commonly used data
    amongst ``/archive/<proposal>/`` and
    ``/archive/<proposal>/<filename>`` webpages.

    Parameters
    ----------
    proposal : str
        The proposal number (e.g. ``12345``).
    fits_type : str
        The FITS type.  Can be ``raw``, ``flt``, or ``flc``.

    Returns
    -------
    data_dict : dict
        A dictionary containing data used to render a webpage.
    """

    data_dict = {}
    data_dict['proposal_id'] = proposal
    data_dict = _get_image_lists(data_dict, fits_type)
    data_dict['rootnames'] = [os.path.basename(item).split('_')[0][:-1] for item in data_dict['jpegs']]
    data_dict['filenames'] = [os.path.basename(item).split('_')[0] for item in data_dict['jpegs']]
    data_dict['num_images'] = len(data_dict['jpegs'])
    data_dict = _get_proposal_status(data_dict)

    return data_dict


# def get_view_header_dict(filename, fits_type='flt'):
#     """
#     """

#     header_dict = {}
#     header_dict['filename'] = filename
#     header_dict['fits_type'] = fits_type.upper

#     return header_dict

def get_view_image_dict(proposal, filename, fits_type='flt'):
    """Return a dictionary containing data used for the
    ``/archive/<proposal>/<filename>/`` webpage.

    Parameters
    ----------
    proposal : str
        The proposal number (e.g. ``12345``).
    filename : str
        The 9-character IPPPSSOOT rootname (e.g. ``jcye04zsq``.)
    fits_type : str
        The JPEG FITS type to view. Can either be ``raw``, ``flt``, or
        ``flc``.

    Returns
    -------
    image_dict : dict
        A dictionary containing data used for the
        ``/archive/<proposal>/<filename>`` webpage.
    """

    image_dict = _initialize_data_dict(proposal, fits_type)
    image_dict['fits_type'] = fits_type.upper()
    image_dict['filename'] = filename
    image_dict['rootname'] = filename[:-1]
    image_dict = _get_metadata_from_database(image_dict)
    image_dict['index'] = image_dict['filenames'].index(image_dict['filename'])
    image_dict['page'] = image_dict['index'] + 1
    image_dict['expstart'] = image_dict['expstarts'][image_dict['index']]
    image_dict['filter1'] = image_dict['filter1s'][image_dict['index']]
    image_dict['filter2'] = image_dict['filter2s'][image_dict['index']]
    image_dict['aperture'] = image_dict['apertures'][image_dict['index']]
    image_dict['exptime'] = image_dict['exptimes'][image_dict['index']]
    image_dict['expflag'] = image_dict['expflags'][image_dict['index']]
    image_dict['quality'] = image_dict['qualitys'][image_dict['index']]
    image_dict['ra'] = image_dict['ras'][image_dict['index']]
    image_dict['dec'] = image_dict['decs'][image_dict['index']]
    image_dict['targname'] = image_dict['targnames'][image_dict['index']]
    image_dict['pi_first_name'] = image_dict['pi_firsts'][image_dict['index']]
    image_dict['pi_last_name'] = image_dict['pi_lasts'][image_dict['index']]
    image_dict['view_url'] = 'archive/{}/{}/{}'.format(image_dict['proposal_id'], image_dict['filename'], fits_type)
    image_dict['fits_links'] = {}
    image_dict['first'] = image_dict['index'] == 0
    image_dict['last'] = image_dict['index'] == image_dict['num_images'] - 1

    # Determine path to JPEG
    jpeg_path = '/static/img/jpegs/{}/{}_{}.jpg'.format(image_dict['proposal_id'], image_dict['filename'], fits_type)
    jpeg_path_abs = os.path.join(SETTINGS['jpeg_dir'], image_dict['proposal_id'], '{}_{}.jpg'.format(image_dict['filename'], fits_type))
    if os.path.exists(jpeg_path_abs):
        image_dict['image'] = jpeg_path
    else:
        image_dict['image'] = None

    # Determine next and previous images, if possible
    if not image_dict['last']:
        image_dict['next'] = {'proposal': image_dict['proposal_id'], 'filename': image_dict['filenames'][image_dict['index'] + 1], 'fits_type': fits_type}
    if not image_dict['first']:
        image_dict['prev'] = {'proposal': image_dict['proposal_id'], 'filename': image_dict['filenames'][image_dict['index'] - 1], 'fits_type': fits_type}

    # Determine other available JPEGs for given observation
    jpeg_types = glob.glob(jpeg_path_abs.replace('{}.jpg'.format(fits_type), '*.jpg'))
    jpeg_types = [os.path.basename(item).split('_')[-1].split('.')[0] for item in jpeg_types]
    jpeg_types = [item for item in jpeg_types if item.upper() != image_dict['fits_type']]
    image_dict['available_jpegs'] = {}
    for jpeg_type in jpeg_types:
        image_dict['available_jpegs'][jpeg_type] = image_dict['view_url'].replace(fits_type, jpeg_type)

    # For downloading the files
    # image_dict['proposal_name'] = image_dict['filename'][0:4]
    # image_dict['fits_links']['FLT'] = os.path.join(
    #                                       SETTINGS['filesystem'],
    #                                       image_dict['proposal_name'],
    #                                       image_dict['filename'],
    #                                       '{}_flt.fits'.format(image_dict['filename']))

    return image_dict


def get_view_proposal_dict(proposal):
    """Return a dictionary containing data used for the
    ``/archive/<proposal>/` webpage.

    Parameters
    ----------
    proposal : str
        The proposal number (e.g. ``12345``).

    Returns
    -------
    proposal_dict : dict
        A dictionary containing data used for the
        ``/archive/<proposal>/`` webpage.
    """

    proposal_dict = _initialize_data_dict(proposal)
    proposal_dict['visits'] = [os.path.basename(item).split('_')[0][4:6].upper() for item in proposal_dict['jpegs']]
    proposal_dict['num_visits'] = len(set(proposal_dict['visits']))
    proposal_dict = _get_metadata_from_database(proposal_dict)
    proposal_dict = _get_buttons_dict(proposal_dict)
    proposal_dict['viewlinks'] = ['/archive/{}/{}/'.format(proposal_dict['proposal_id'], filename) for filename in proposal_dict['filenames']]

    return proposal_dict


def get_view_query_results_dict(query_results_dict):
    """Return a dictionary containing data used for the
    ``/database/results/`` webpage.

    Parameters
    ----------
    query_results_dict : dict
        A dictionary containing the results of the query performed
        through the ``/database/`` webpage, along with some additional
        metadata.

    Returns
    -------
    thumbnail_dict : dict
        A dictionary containing data used for the ``/database/results/``
        webpage.
    """

    query_results = query_results_dict['query_results']

    thumbnail_dict = {}
    thumbnail_dict['num_images'] = query_results_dict['num_results']
    thumbnail_dict['rootnames'] = [item[3] for item in query_results]
    thumbnail_dict['filenames'] = [item[4].split('_')[0] for item in query_results]
    thumbnail_dict['detectors'] = [item[5] for item in query_results]
    thumbnail_dict['expstarts'] = [item[6] for item in query_results]
    thumbnail_dict['filter1s'] = [item[7] for item in query_results]
    thumbnail_dict['filter2s'] = [item[8] for item in query_results]
    thumbnail_dict['exptimes'] = [item[9] for item in query_results]
    thumbnail_dict['targnames'] = [item[10] for item in query_results]
    thumbnail_dict['proposal_ids'] = [item[11] for item in query_results]
    thumbnail_dict['visits'] = [item[4:6] for item in thumbnail_dict['rootnames']]
    thumbnail_dict = _get_buttons_dict(thumbnail_dict)
    thumbnail_dict['thumbs'] = ['static/img/thumbnails/{}/{}_flt.thumb'.format(proposid, filename)
        for proposid, filename in zip(thumbnail_dict['proposal_ids'], thumbnail_dict['filenames'])]
    thumbnail_dict['viewlinks'] = ['/archive/{}/{}/'.format(proposid, filename)
        for proposid, filename in zip(thumbnail_dict['proposal_ids'], thumbnail_dict['filenames'])]

    return thumbnail_dict
