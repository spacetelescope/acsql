import glob
import html
import os
import requests

from acsql.database import database_interface
from acsql.database.database_interface import Master


def get_image_lists(data_dict):
    """
    """

    jpeg_proposal_path = os.path.join('static/img/jpegs/', data_dict['proposal_id'])
    thumb_proposal_path = os.path.join('static/img/thumbnails/', data_dict['proposal_id'])

    data_dict['raw_jpegs'] = glob.glob(os.path.join(jpeg_proposal_path, '*raw.jpg'))
    data_dict['flt_jpegs'] = glob.glob(os.path.join(jpeg_proposal_path, '*flt.jpg'))
    data_dict['flc_jpegs'] = glob.glob(os.path.join(jpeg_proposal_path, '*flc.jpg'))
    data_dict['thumbs'] = glob.glob(os.path.join(thumb_proposal_path, '*flt.thumb'))

    return data_dict


def get_proposal_buttons_dict(proposal_dict):
    """
    """

    proposal_dict['buttons'] = {}
    proposal_dict['buttons']['detector'] = sorted(set(proposal_dict['detectors']))
    proposal_dict['buttons']['visit'] = sorted(set(proposal_dict['visits']))
    proposal_dict['buttons']['target'] = sorted(set(proposal_dict['targnames']))
    proposal_dict['buttons']['filter'] = sorted(set([
        '{}/{}'.format(filter1, filter2)
        for filter1, filter2
        in zip(proposal_dict['filter1s'], proposal_dict['filter2s'])]))

    return proposal_dict


def get_proposal_metadata(proposal_dict):
    """
    """

    proposal_dict['detectors'] = []
    proposal_dict['exptimes'] = []
    proposal_dict['filter1s'] = []
    proposal_dict['filter2s'] = []
    proposal_dict['targnames'] = []
    proposal_dict['expstarts'] = []

    session = getattr(database_interface, 'session')

    for rootname in proposal_dict['rootnames']:

        detector = session.query(Master.detector)\
            .filter(Master.rootname == rootname).one()[0]

        table = getattr(database_interface, '{}_raw_0'.format(detector))
        results = session.query(
            table.exptime, table.filter1, table.filter2, table.targname, getattr(table, 'date-obs'), getattr(table, 'time-obs'))\
            .filter(table.rootname == rootname).one()

        proposal_dict['detectors'].append(detector)
        proposal_dict['exptimes'].append(results[0])
        proposal_dict['filter1s'].append(results[1])
        proposal_dict['filter2s'].append(results[2])
        proposal_dict['targnames'].append(results[3])
        proposal_dict['expstarts'].append(results[4])

    session.close()

    return proposal_dict


def initialize_data_dict(proposal):
    """
    """

    data_dict = {}
    data_dict['proposal_id'] = proposal
    data_dict = get_image_lists(data_dict)
    data_dict['num_images'] = len(data_dict['flt_jpegs'])
    data_dict = get_proposal_status(data_dict)

    return data_dict


def get_proposal_status(data_dict):
    """
    """

    data_dict['status_page'] = ('http://www.stsci.edu/cgi-bin/get-proposal'
        '-info?id={}&submit=Go&observatory=HST').format(data_dict['proposal_id'])

    req = requests.get(data_dict['status_page'], timeout=3)

    if req.ok:

        status_string = req.content.decode() # get HTML content of page
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


def get_view_image_dict(proposal, rootname):
    """
    """

    image_dict = initialize_data_dict(proposal)
    image_dict['rootname'] = rootname
    image_dict['page'] # The index of the image in the list

    return image_dict


def get_view_proposal_dict(proposal):
    """
    """

    proposal_dict = initialize_data_dict(proposal)
    proposal_dict['rootnames'] = [os.path.basename(item).split('_')[0][:-1] for item in proposal_dict['flt_jpegs']]
    proposal_dict['visits'] = [os.path.basename(item).split('_')[0][4:6].upper() for item in proposal_dict['flt_jpegs']]
    proposal_dict['num_visits'] = len(set(proposal_dict['visits']))
    proposal_dict = get_proposal_metadata(proposal_dict)
    proposal_dict = get_proposal_buttons_dict(proposal_dict)
    proposal_dict['viewlinks'] = ['localhost:5000/archive/{}/{}'.format(proposal_dict['proposal_id'], rootname) for rootname in proposal_dict['rootnames']]

    return proposal_dict
