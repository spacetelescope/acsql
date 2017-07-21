from wtforms import widgets, TextField, validators, SelectMultipleField, Form, FormField, SelectField, DateField, DecimalField, RadioField
from wtforms_components.fields import IntegerField

from acsql.website.form_choices import FORM_CHOICES


operator_form = SelectField('Operator',
                [validators.Optional()],
                choices=[('=', '='), ('<', '<'), ('>','>'), ('between','between')],
                default=('=', '='))


class CheckboxField(SelectMultipleField):
    """
    Like a SelectField, except displays a list of checkbox buttons.
    Iterating the field will produce subfields (each containing a label as
    well) in order to allow custom rendering of the individual radio fields.
    """

    widget = widgets.ListWidget(prefix_label=False)
    option_widget = widgets.CheckboxInput()


class DateForm(Form):
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
  op = operator_form
  val1 = DecimalField('Exposure Time', [validators.Optional()])
  val2 = DecimalField('exptime2', [validators.Optional()])


class MultiCheckboxField(SelectMultipleField):
    """
    A multiple-select, except displays a list of checkboxes.
    Iterating the field will produce subfields, allowing custom rendering of
    the enclosed checkbox fields.
    """

    widget = widgets.ListWidget(prefix_label=False)
    option_widget = widgets.CheckboxInput()


class RequiredIf(validators.Required):
  """
  Custom validator to enforce requires only if another field matches a
  specified value. the `negate` allows for inverting the result.
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
                    choices=FORM_CHOICES['proposal_type'])
    detector = CheckboxField('Detector',
               [validators.Optional()],
               description='span3',
               choices=FORM_CHOICES['detector'])
    obstype = CheckboxField('Observation Type',
              [validators.Optional()],
              description='span3',
              choices=FORM_CHOICES['obstype'])
    aperture = SelectMultipleField('Aperture',
               [validators.Optional()],
               choices=FORM_CHOICES['aperture'],
               description='span3')
    filter1 = SelectMultipleField('Filter1',
              [validators.Optional()],
              description='span3',
              choices=FORM_CHOICES['filter1'])
    filter2 = SelectMultipleField('Filter2',
              [validators.Optional()],
              description='span3',
              choices=FORM_CHOICES['filter2'])
    imagetyp = SelectMultipleField('Image Type',
               [validators.Optional()],
               choices=FORM_CHOICES['imagetyp'],
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
                     choices=FORM_CHOICES['output_columns'])
    output_format = RadioField('Output Format',
                    [validators.Required(message='Please select an output format.')],
                    choices=FORM_CHOICES['output_format'],
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
