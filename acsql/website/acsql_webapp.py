#! /usr/bin/env python

"""This module serves as the ``acsql`` web application, which allows
users to view image data and interact with the ``acsql`` database.

The module is build using the ``flask`` python web framework.  See
accompanying ``data_containers``, ``query_form``, and ``form_choices``
modules for further information.

Authors
-------

    - Matthew Bourque
    - Meredith Durbin

Use
---

    This module is intended to be executed on a web server, but it can
    also be run locally:
    ::

        python acsql_webapp.py
        <go to localhost:5000 in a browser>

Dependencies
------------

    - acsql
    - flask
    - numpy
"""

import glob
import os

from flask import Flask, render_template
import numpy as np

from acsql.utils.utils import SETTINGS
from acsql.website.data_containers import get_view_image_dict
from acsql.website.data_containers import get_view_proposal_dict
from acsql.website.query_form import get_query_form

app = Flask(__name__)


@app.route('/archive/')
def archive():
    """Returns webpage containing links to all ACS archive proposals.

    Returns
    -------
    template : obj
        The ``archive.html`` template.
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


@app.route('/database/')
def database():
    """Returns webpage containing a query form for querying the
    ``acsql`` database.

    Returns
    -------
    template : obj
        The ``database.html`` webpage.
    """

    query_form = get_query_form()
    return render_template('database.html', form=query_form)


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


# @app.route('/archive/<proposal>/<filename>/<fits_type/header/')
# def view_header(proposal, filename, fits_type):
#     """
#     """

#     header_dict = get_view_header_dict()
#     return render_template('header.html', header_dict=header_dict)


@app.route('/archive/<proposal>/<filename>/')
@app.route('/archive/<proposal>/<filename>/<fits_type>/')
def view_image(proposal, filename, fits_type='flt'):
    """Returns webpage for viewing a single JPEG image, along with some
    useful metadata and links to additional information/downloads.

    If an invalid ``fits_type`` is supplied, a 404 page is returned.

    Parameters
    ----------
    proposal : str
        The proposal ID (e.g. ``'12345'``).
    filename : str
        The 9-character IPPPSSOOT rootname (e.g. ``jcye04zsq``.)
    fits_type : str
        The JPEG FITS type to view. Can either be ``raw``, ``flt``, or
        ``flc``.

    Returns
    -------
    template : obj
        The ``view_image.html`` template.
    """

    if fits_type in ['raw', 'flt', 'flc']:
        image_dict = get_view_image_dict(proposal, filename, fits_type)
        return render_template('view_image.html', image_dict=image_dict)
    else:
        return render_template('404.html'), 404


@app.route('/archive/<proposal>/')
def view_proposal(proposal):
    """Returns webpage for viewing all thumbnails for a given
    ``proposal``, along with some metadata and links to additional
    information.

    If an invalid proposal is supplied, a 404 page is returned.

    Parameters
    ----------
    proposal : str
        The proposal ID (e.g. ``'12345'``).

    Returns
    -------
    template : obj
        The ``view_proposal.html`` template.
    """

    proposal_list = glob.glob(os.path.join(SETTINGS['jpeg_dir'], '*'))
    proposal_list = [item.split('/')[-1] for item in proposal_list]

    if proposal in proposal_list:
        proposal_dict = get_view_proposal_dict(proposal)
        return render_template('view_proposal.html', proposal_dict=proposal_dict)
    else:
        return render_template('404.html'), 404


if __name__ == '__main__':

    app.run()
