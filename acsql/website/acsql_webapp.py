#! /usr/bin/env python

import glob

from flask import Flask, render_template
import numpy as np

from acsql.utils.utils import SETTINGS

app = Flask(__name__)


@app.route('/archive/')
def archive():
    """
    """

    proposal_list = glob.glob(os.path.join(SETTINGS['jpeg_dir'], '*'))

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


if __name__ == '__main__':

    app.run()
