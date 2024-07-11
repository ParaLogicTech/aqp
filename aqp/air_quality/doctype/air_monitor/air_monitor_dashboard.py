from frappe import _


def get_data():
	return {
		'fieldname': 'air_monitor',
		'transactions': [
			{
				'label': _('Readings'),
				'items': ['Monitor Reading']
			},
		]
	}
