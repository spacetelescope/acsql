import glob
import html
import os
import requests

from acsql.database import database_interface
from acsql.database.database_interface import Master
from acsql.utils.utils import SETTINGS


def get_image_lists(data_dict):
    """
    """

    jpeg_proposal_path = os.path.join('static/img/jpegs/', data_dict['proposal_id'])
    thumb_proposal_path = os.path.join('static/img/thumbnails/', data_dict['proposal_id'])

    data_dict['raw_jpegs'] = sorted(glob.glob(os.path.join(jpeg_proposal_path, '*raw.jpg')))
    data_dict['flt_jpegs'] = sorted(glob.glob(os.path.join(jpeg_proposal_path, '*flt.jpg')))
    data_dict['flc_jpegs'] = sorted(glob.glob(os.path.join(jpeg_proposal_path, '*flc.jpg')))
    data_dict['thumbs'] = sorted(glob.glob(os.path.join(thumb_proposal_path, '*flt.thumb')))

    return data_dict


def get_metadata_from_database(data_dict):
    """
    """

    session = getattr(database_interface, 'session')

    results = []
    for rootname in data_dict['rootnames']:

        detector = session.query(Master.detector)\
            .filter(Master.rootname == rootname).one()[0]

        table = getattr(database_interface, '{}_raw_0'.format(detector))
        result = session.query(
            table.aperture, table.exptime, table.filter1, table.filter2, \
            table.targname, getattr(table, 'date-obs'), \
            getattr(table, 'time-obs'), table.expstart, table.expflag, \
            table.quality, table.ra_targ, table.dec_targ, table.pr_inv_f, \
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


def initialize_data_dict(proposal):
    """
    """

    data_dict = {}
    data_dict['proposal_id'] = proposal
    data_dict = get_image_lists(data_dict)
    data_dict['rootnames'] = [os.path.basename(item).split('_')[0][:-1] for item in data_dict['flt_jpegs']]
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
    image_dict = get_metadata_from_database(image_dict)
    image_dict['rootname'] = rootname
    image_dict['index'] = image_dict['rootnames'].index(image_dict['rootname'])
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
    image_dict['image'] = 'static/img/jpegs/{}/{}_flt.jpg'.format(image_dict['proposal_id'], image_dict['rootname'])
    image_dict['view_url'] = 'archive/{}/{}'.format(image_dict['proposal_id'], image_dict['rootname'])
    image_dict['fits_links'] = {}
    image_dict['proposal_name'] = image_dict['rootname'][0:4]
    image_dict['flt'] = os.path.join(
                            SETTINGS['filesystem'],
                            image_dict['proposal_name'],
                            image_dict['rootname'],
                            '{}_flt.fits'.format(image_dict['rootname']))

    return image_dict


def get_view_proposal_dict(proposal):
    """
    """

    proposal_dict = initialize_data_dict(proposal)
    proposal_dict['visits'] = [os.path.basename(item).split('_')[0][4:6].upper() for item in proposal_dict['flt_jpegs']]
    proposal_dict['num_visits'] = len(set(proposal_dict['visits']))
    proposal_dict = get_metadata_from_database(proposal_dict)
    proposal_dict = get_proposal_buttons_dict(proposal_dict)
    proposal_dict['viewlinks'] = ['localhost:5000/archive/{}/{}'.format(proposal_dict['proposal_id'], rootname) for rootname in proposal_dict['rootnames']]

    return proposal_dict
