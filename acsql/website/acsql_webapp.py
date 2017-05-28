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


def get_image_lists(image_dict):
    """
    """

    jpeg_proposal_path = os.path.join(SETTINGS['jpeg_dir'], proposal)
    thumb_proposal_path = os.path.join(SETTINGS['thumbnail_dir'], proposal)

    image_dict['raw_jpegs'] = glob.glob(os.path.join(jpeg_proposal_path, '*raw.jpg'))
    image_dict['flt_jpegs'] = glob.glob(os.path.join(jpeg_proposal_path, '*flt.jpg'))
    image_dict['flc_jpegs'] = glob.glob(os.path.join(jpeg_proposal_path, '*flc.jpg'))

    image_dict['raw_thumbs'] = glob.glob(os.path.join(thumb_proposal_path, '*raw.thumb'))
    image_dict['flt_thumbs'] = glob.glob(os.path.join(thumb_proposal_path, '*flt.thumb'))
    image_dict['flc_thumbs'] = glob.glob(os.path.join(thumb_proposal_path, '*flc.thumb'))

    return image_dict


def get_keyword_metadata(image_dict):
    """
    """

    image_dict['exptimes'] = []
    image_dict['filter1s'] = []
    image_dict['filter2s'] = []
    image_dict['targnames'] = []
    image_dict['expstarts'] = []

    session = getattr(database_interface, 'session')

    for rootname in image_dict['rootnames']:

        detector = session.query(Master.detector)\
            .filter(Master.rootname == rootname).one()

        table = getattr(database_interface, '{}_raw_0'.format(detector))
        results = session.query(
            table.exptime, table.filter1, table.filter2, table.targname, table.date_obs, table.time_obs)\
            .filter(table.rootname == rootname).one()

        image_dict['exptimes'].append(results[0])
        image_dict['filter1s'].append(results[1])
        image_dict['filter2s'].append(results[2])
        image_dict['targnames'].append(results[3])
        image_dict['expstarts'].append(results[4])

    session.close()

    return image_dict

def get_proposal_status(image_dict):
    """
    """

    image_dict['status_page'] = ('http://www.stsci.edu/cgi-bin/get-proposal'
        '-info?id={}&submit=Go&observatory=HST').format(proposal)

    req = requests.get(image_dict['status_page'], timeout=3) # timeout in 3 seconds

    if req.ok: # quick way of checking that page we're trying to access is valid

        status_string = req.content.decode() # get HTML content of page
        image_dict['proposal_title'] = html.unescape(status_string.\
            split('<b>Title:</b> ')[1].\
            split('<br>')[0])
        image_dict['cycle'] = html.unescape(status_string.\
            split('<b>Cycle:</b> ')[1].\
            split('<br>')[0])
        image_dict['schedule'] = html.unescape(status_string.\
            split('/proposal-help-HST.html#')[2].\
            split('">')[0])

    else:
        print('Request failed: {}'.format(image_dict['status_page']))
        image_dict['proposal_title'] = 'proposal title unavailable'
        image_dict['cycle'] = None
        image_dict['schedule'] = None

    return image_dict


@app.route('/archive/')
def archive():
    """
    """

    # Get list of all proposal numbers
    proposal_list = glob.glob(os.path.join(SETTINGS['jpeg_dir'], '*'))
    proposal_list = sorted([os.path.basename(item) for item in proposal_list])

    # rearrange list so that it appears in six columns
    ncols = 6
    if len(proposal_list) % ncols != 0:
            proposal_list.extend([''] * (ncols - (len(proposal_list) % ncols)))
    proposal_array = np.asarray(proposal_list).reshape(ncols, len(proposal_list) / ncols).T

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

    image_dict = {}
    image_dict = get_image_lists(image_dict)
    image_dict['proposal_id'] = proposal
    image_dict['num_images'] = len(flt_jpeg_list)
    image_dict['rootnames'] = [os.path.basename(item).split('_')[0][:-1] for item in image_dict['flt_jpegs']]
    image_dict['visits'] = [os.path.basename(item).split('_')[0][4:6] for item in image_dict['flt_jpegs']]
    image_dict = get_proposal_status(image_dict)
    image_dict = get_keyword_metadata(img_dict)

    return render_template('404.html')


if __name__ == '__main__':

    app.run()
