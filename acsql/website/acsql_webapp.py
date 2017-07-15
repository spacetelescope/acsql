#! /usr/bin/env python

import glob
import os

from flask import Flask, render_template
import numpy as np

from acsql.utils.utils import SETTINGS
from acsql.website.data_containers import get_view_image_dict
from acsql.website.data_containers import get_view_proposal_dict

app = Flask(__name__)


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


@app.route('/archive/<proposal>/<rootname>/')
def view_image(proposal, rootname):
    """
    """

    image_dict = get_view_image_dict(proposal, rootname)
    return render_template('view_image.html', image_dict=image_dict)


@app.route('/archive/<proposal>/')
def view_proposal(proposal):
    """
    """

    proposal_dict = get_view_proposal_dict(proposal)
    return render_template('view_proposal.html', proposal_dict=proposal_dict)


if __name__ == '__main__':

    app.run()
