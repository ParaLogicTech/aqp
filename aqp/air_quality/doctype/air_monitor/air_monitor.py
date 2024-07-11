# Copyright (c) 2023, ParaLogic and contributors
# For license information, please see license.txt

import frappe
from frappe.utils import clean_whitespace, cint
from aqp.air_quality.utils import get_order_by
from frappe.model.document import Document


class AirMonitor(Document):
	def before_insert(self):
		self.monitor_name = clean_whitespace(self.monitor_name)

	def validate(self):
		self.clean_fields()
		self.set_first_last_reading()

	def on_update(self):
		clear_monitors_cache()

	def on_trash(self):
		clear_monitors_cache()

	def clean_fields(self):
		fields = ["city", "serial_no"]
		for f in fields:
			self.set(f, clean_whitespace(self.get(f)))

	def set_first_last_reading(self, update=True, update_modified=False):
		query = """
			select reading_dt
			from `tabMonitor Reading`
			where air_monitor = %s
			order by reading_dt {0}
			limit 1
		"""

		first = frappe.db.sql(query.format("asc"), self.name)
		last = frappe.db.sql(query.format("desc"), self.name)

		self.first_reading_dt = first[0][0] if first else None
		self.last_reading_dt = last[0][0] if last else None

		if update:
			self.db_set({
				"first_reading_dt": self.first_reading_dt,
				"last_reading_dt": self.last_reading_dt,
			}, update_modified=update_modified)


@frappe.whitelist()
def get_monitors(filters=None, limit_start=0, limit_page_length=20, sort_by="creation", sort_order="asc"):
	monitors = _get_monitors(filters, limit_start, limit_page_length, sort_by, sort_order)

	return frappe._dict({
		"data": monitors,
		"pagination": frappe._dict({
			"count": len(monitors),
			"total_count": frappe.db.count("Air Monitor", filters),
			"limit_start": cint(limit_start),
			"limit_page_length": cint(limit_page_length),
		}),
	})


def _get_monitors(filters=None, limit_start=None, limit_page_length=None, sort_by=None, sort_order=None):
	fields = [
		"name", "monitor_name", "inactive",
		"country", "city", "latitude", "longitude",
		"online_since", "first_reading_dt", "last_reading_dt",
		"creation", "modified"
	]

	if not filters:
		filters = {}

	if isinstance(filters, dict):
		filters["disabled"] = 0
	elif isinstance(filters, list):
		filters.append(["Air Monitor", "disabled", "=", 0])

	return frappe.get_all(
		"Air Monitor",
		fields=fields,
		filters=filters,
		limit_start=limit_start,
		limit_page_length=limit_page_length,
		order_by=get_order_by("Air Monitor", sort_by, sort_order, fields)
	)


def clear_monitors_cache():
	pass
