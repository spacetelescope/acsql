#! /usr/bin/env python

"""Create plot that shows the size of the filesystem over time.

Authors
-------

    Matthew Bourque

Use
---

    This script is intended to be executed via the command line as
    such:
    ::

        python plot_file_sizes.py

Dependencies
------------

    - ``matplotlib``
"""

import datetime
import matplotlib.pyplot as plt


def main():
    """The main function."""

    # Read in the data
    with open('figures/file_sizes.dat', 'r') as f:
        data = f.readlines()
    data = [item.strip().split(',') for item in data]
    dates = [item[0] for item in data]
    sizes = [float(item[1]) for item in data]
    detectors = [item[2] for item in data]

    # Sort the data by date
    dates, sizes, detectors = (list(x) for x in zip(*sorted(zip(dates, sizes, detectors))))

    # Convert the dates to datetime objects
    dates = [datetime.datetime.strptime(date, '%Y-%m-%d') for date in dates]

    # Get list of aggregate sizes
    agg_sizes_all, agg_sizes_wfc, agg_sizes_hrc, agg_sizes_sbc = [], [], [], []
    agg_size_all, agg_size_wfc, agg_size_hrc, agg_size_sbc = 0, 0, 0, 0
    dates_all, dates_wfc, dates_hrc, dates_sbc = [], [], [], []

    for size, date, detector in zip(sizes, dates, detectors):
        dates_all.append(date)
        agg_size_all += size
        agg_sizes_all.append(agg_size_all)

        if detector == 'WFC':
            dates_wfc.append(date)
            agg_size_wfc += size
            agg_sizes_wfc.append(agg_size_wfc)

        if detector == 'HRC':
            dates_hrc.append(date)
            agg_size_hrc += size
            agg_sizes_hrc.append(agg_size_hrc)

        if detector == 'SBC':
            dates_sbc.append(date)
            agg_size_sbc += size
            agg_sizes_sbc.append(agg_size_sbc)

    # Plot the data
    plt.rcParams['font.size'] = 14
    plt.style.use('bmh')
    fig = plt.figure(figsize=(8, 6))
    ax = fig.add_subplot(111)
    ax.set_title('Total Size of acsql Filesystem')
    ax.set_ylabel('Size (TB)')
    ax.plot(dates_all, agg_sizes_all, linewidth=3, label='Total')
    ax.plot(dates_wfc, agg_sizes_wfc, linewidth=3, label='WFC')
    ax.plot(dates_hrc, agg_sizes_hrc, linewidth=3, label='HRC')
    ax.plot(dates_sbc, agg_sizes_sbc, linewidth=3, label='SBC')
    plt.legend()
    plt.tight_layout()
    plt.savefig('figures/filesystem_size.png')


if __name__ == '__main__':

    main()
