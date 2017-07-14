#! /usr/bin/env python

import glob
import html
import os
import requests

from flask import Flask, render_template
import numpy as np

from acsql.database import database_interface
from acsql.database.database_interface import Master
from acsql.utils.utils import SETTINGS

app = Flask(__name__)


def get_buttons_dict(proposal_dict):
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

def get_image_lists(proposal_dict):
    """
    """

    jpeg_proposal_path = os.path.join(SETTINGS['jpeg_dir'], proposal_dict['proposal_id'])
    thumb_proposal_path = os.path.join(SETTINGS['thumbnail_dir'], proposal_dict['proposal_id'])

    proposal_dict['raw_jpegs'] = glob.glob(os.path.join(jpeg_proposal_path, '*raw.jpg'))
    proposal_dict['flt_jpegs'] = glob.glob(os.path.join(jpeg_proposal_path, '*flt.jpg'))
    proposal_dict['flc_jpegs'] = glob.glob(os.path.join(jpeg_proposal_path, '*flc.jpg'))
    proposal_dict['thumbs'] = glob.glob(os.path.join(thumb_proposal_path, '*flt.thumb'))

    return proposal_dict


def get_keyword_metadata(proposal_dict):
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

def get_proposal_status(proposal_dict):
    """
    """

    proposal_dict['status_page'] = ('http://www.stsci.edu/cgi-bin/get-proposal'
        '-info?id={}&submit=Go&observatory=HST').format(proposal_dict['proposal_id'])

    req = requests.get(proposal_dict['status_page'], timeout=3)

    if req.ok:

        status_string = req.content.decode() # get HTML content of page
        proposal_dict['proposal_title'] = html.unescape(status_string.\
            split('<b>Title:</b> ')[1].\
            split('<br>')[0])
        proposal_dict['cycle'] = html.unescape(status_string.\
            split('<b>Cycle:</b> ')[1].\
            split('<br>')[0])
        proposal_dict['schedule'] = html.unescape(status_string.\
            split('/proposal-help-HST.html#')[2].\
            split('">')[0])

    else:
        print('Request failed: {}'.format(proposal_dict['status_page']))
        proposal_dict['proposal_title'] = 'proposal title unavailable'
        proposal_dict['cycle'] = None
        proposal_dict['schedule'] = None

    return proposal_dict


@app.route('/archive/')
def archive():
    """
    """

    # Get list of all proposal numbers
    proposal_list = glob.glob(os.path.join(SETTINGS['jpeg_dir'], '*'))
    proposal_list = sorted([int(os.path.basename(item)) for item in proposal_list])

    # rearrange list so that it appears in multiple columns
    ncols = 12
    if len(proposal_list) % ncols != 0:
            proposal_list.extend([''] * (ncols - (len(proposal_list) % ncols)))
    proposal_array = np.asarray(proposal_list).reshape(ncols, int(len(proposal_list) / ncols)).T

    return render_template('archive.html', proposal_array=proposal_array)


def handle_500(trace):
    """Handle 500 error.

    Parameters
    ----------
    trace : str
        The traceback of the error.

    Returns
    -------
    template : obj
        The ``500.html`` template.
    """

    trace_html = trace.replace('\n', '<br>')

    return render_template('500.html', trace_html=trace_html)


@app.route('/')
def main():
    """Generates the ``acsql`` website homepage.

    Returns
    -------
    template : obj
        The ``index.html`` template.
    """

    return render_template('index.html')


@app.errorhandler(404)
def page_not_found(error):
    """Redirects any nonexistent URL to 404 page.

    Parameters
    ----------
    error : obj
        The ``error`` thrown.

    Returns
    -------
    template : obj
        The ``404.html`` template.
    """

    return render_template('404.html'), 404


@app.route('/archive/<proposal>/')
def view_proposal(proposal):
    """
    """

    proposal_dict = {}
    proposal_dict['proposal_id'] = proposal
    proposal_dict = get_image_lists(proposal_dict)
    proposal_dict['num_images'] = len(proposal_dict['flt_jpegs'])
    proposal_dict['rootnames'] = [os.path.basename(item).split('_')[0][:-1] for item in proposal_dict['flt_jpegs']]
    proposal_dict['visits'] = [os.path.basename(item).split('_')[0][4:6].upper() for item in proposal_dict['flt_jpegs']]
    proposal_dict['num_visits'] = len(set(proposal_dict['visits']))
    proposal_dict = get_proposal_status(proposal_dict)
    proposal_dict = get_keyword_metadata(proposal_dict)
    proposal_dict = get_buttons_dict(proposal_dict)
    proposal_dict['viewlinks'] = ['localhost:5000/archive/{}/{}'.format(proposal_dict['proposal_id'], rootname) for rootname in proposal_dict['rootnames']]

    print(proposal_dict)
    return render_template('view_proposal.html', proposal_dict=proposal_dict)


if __name__ == '__main__':

    app.run()
