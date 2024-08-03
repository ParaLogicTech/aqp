from frappe import _


def get_data():
	return {
		'fieldname': 'monitor_region',
		'transactions': [
			{
				'label': _('Monitors'),
				'items': ['Air Monitor']
			},
			{
				'label': _('Readings'),
				'items': ['Reading Aggregate']
			},
		]
	}
