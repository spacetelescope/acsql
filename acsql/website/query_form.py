"""Contains class objects for building a query form for querying the
``acsql`` database through the ``acsql`` web application.

Many of the class objects are subclasses or extensions from components
provided by the ``wtforms`` library.  Hard coded data such as form
options are imported from the ``form_options`` module.

Authors
-------

    - Matthew Bourque
    - Meredith Durbin

Use
---

    This module is inteded to be imported and used by the
    ``acsql_webapp`` module as such:
    ::

        from acsql.website.query_form import get_query_form
        query_form = get_query_form()

Dependencies
------------

    - acsql
    - wtforms
    - wtforms_components
"""

from wtforms import DateField
from wtforms import DecimalField
from wtforms import Form
from wtforms import FormField
from wtforms import RadioField
from wtforms import SelectField
from wtforms import SelectMultipleField
from wtforms import TextField
from wtforms import validators
from wtforms import widgets
from wtforms_components.fields import IntegerField

from acsql.website.form_options import FORM_OPTIONS


operator_form = SelectField('Operator',
                [validators.Optional()],
                choices=[('=', '='), ('<', '<'), ('>','>'), ('between','between')],
                default=('=', '='))


class CheckboxField(SelectMultipleField):
    """Like a ``SelectField``, except displays a list of checkbox
    buttons.

    Parameters
    ----------
    SelectMultipleField : obj
        The ``SelectMultipleField`` object from ``wtforms``
    """

    widget = widgets.ListWidget(prefix_label=False)
    option_widget = widgets.CheckboxInput()


class DateForm(Form):
    """Creates a ``DateForm`` object that allows for date input in a
    form field.

    Parameters
    ----------
    Form : obj
        The ``Form`` object from ``wtforms``.
    """

    op = operator_form
    val1 = DateField('Date Observed',
         [validators.Optional()],
         description='YYYY-MM-DD',
         format='%Y-%m-%d')
    val2 = DateField('dateobs2',
         [validators.Optional()],
         description='YYYY-MM-DD',
         format='%Y-%m-%d')


class ExptimeForm(Form):
    """Creates a ``ExptimeForm`` object that allows for ``exptime``
    input in a form field.

    Parameters
    ----------
    Form : obj
        The ``Form`` object from ``wtforms``.
    """
    op = operator_form
    val1 = DecimalField('Exposure Time', [validators.Optional()])
    val2 = DecimalField('exptime2', [validators.Optional()])


class MultiCheckboxField(SelectMultipleField):
    """A multiple-select, except displays a list of checkboxes.

    Parameters
    ----------
    SelectMultipleField : obj
        The ``SelectMultipleField`` object from ``wtforms``
    """

    widget = widgets.ListWidget(prefix_label=False)
    option_widget = widgets.CheckboxInput()


def is_field_value(form, fieldname, value, negate=False):
    """Helper function to check if the given field in the given form is
    of a specified value.

    Parameters
    ----------
    form: obj
        The form to test on
    fieldname : str
        The fieldname to test value against. If not found an Exception
        is raised.
    value : str
        Value to test for.
    negate : boolean
        True/False to invert the result.
    """

    field = form._fields.get(fieldname)
    if field is None:
        raise Exception('Invalid field "%s"' % fieldname)
    test = value == field.data
    test = not test if negate else test

    return test


class RequiredIf(validators.Required):
    """Custom validator to enforce requires only if another field
    matches a specified value. the ``negate`` allows for inverting
    the result.

    Parameters
    ----------
    validators.Required : obj
        The ``validators.Required`` object from ``wtforms``.
    """

    def __init__(self, other_fieldname, value, negate, *args, **kwargs):
        self.other_fieldname = other_fieldname
        self.negate = negate
        self.value = value
        super(RequiredIf, self).__init__(*args, **kwargs)

    def __call__(self, form, field):
        if is_field_value(form, self.other_fieldname, self.value, self.negate):
            super(RequiredIf, self).__call__(form, field)


class QueryForm(Form):
    """Form for querying the ``acsql`` database.

    Parameters
    ----------
    Form : obj
        The ``Form`` object from ``wtforms``
    """

    rootname = TextField('Rootname',
               [validators.Optional()],
               description='Single rootname (IPPPSSOOT) or comma-separated list span6')
    targname = TextField('Target Name',
               [validators.Optional()],
               description='ex. OMEGACEN, NGC-3603, IRAS05129+5128; single or comma-separated span6')
    proposid = IntegerField('Proposal ID',
               [validators.Optional(),
               validators.NumberRange(min=8183, max=19999,
               message='Please enter a valid proposal ID')],
               description='span4')
    date_obs = FormField(DateForm,
               'Date Observed',
               description='span4')
    exptime = FormField(ExptimeForm,
              'Exposure Time',
              description='span4')
    proposal_type = CheckboxField('Proposal Type',
                    [validators.Optional()],
                    description='span3',
                    choices=FORM_OPTIONS['proposal_type'])
    detector = CheckboxField('Detector',
               [validators.Optional()],
               description='span3',
               choices=FORM_OPTIONS['detector'])
    obstype = CheckboxField('Observation Type',
              [validators.Optional()],
              description='span3',
              choices=FORM_OPTIONS['obstype'])
    aperture = SelectMultipleField('Aperture',
               [validators.Optional()],
               choices=FORM_OPTIONS['aperture'],
               description='span3')
    filter1 = SelectMultipleField('Filter1',
              [validators.Optional()],
              description='span3',
              choices=FORM_OPTIONS['filter1'])
    filter2 = SelectMultipleField('Filter2',
              [validators.Optional()],
              description='span3',
              choices=FORM_OPTIONS['filter2'])
    imagetyp = SelectMultipleField('Image Type',
               [validators.Optional()],
               choices=FORM_OPTIONS['imagetyp'],
               description='span3')
    pr_inv_l = TextField('PI Last Name',
               [validators.Optional()],
               description='Single or comma-separated span3')
    pr_inv_f = TextField('PI First Name',
               [validators.Optional()],
               description='Single or comma-separated span3')
    output_columns = MultiCheckboxField('Output Columns',
                     [RequiredIf('output_format', 'thumbnails', True,
                     message='Please select at least one output column.')],
                     choices=FORM_OPTIONS['output_columns'])
    output_format = RadioField('Output Format',
                    [validators.Required(message='Please select an output format.')],
                    choices=FORM_OPTIONS['output_format'],
                    default='thumbnails', description='span3')


def get_query_form():
    """Return the ``QueryForm`` object that contains query form
    components

    Returns
    -------
    query_form : obj
        The ``QueryForm`` object, which contains the various components
        to build the ACS Database query form.
    """

    query_form = QueryForm()

    return query_form
