# Copyright (c) 2013, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

import frappe
from frappe import _, scrub
from frappe.utils import getdate, flt, cint, add_to_date, add_days
from aqp.air_quality.doctype.monitor_reading.monitor_reading import calculate_aqi


def execute(filters=None):
	return AirQualityAnalytics(filters).run()


class AirQualityAnalytics(object):
	def __init__(self, filters=None):
		self.filters = frappe._dict(filters or {})
		self.months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
		self.get_period_date_ranges()
		self.entity_names = {}
		self.filters.tree_doctype = self.get_tree_doctype()

	def run(self):
		self.get_columns()
		self.get_data()
		self.get_chart_data()
		return self.columns, self.data, None, self.chart

	def get_columns(self):
		self.columns = [{
			"label": _(self.filters.tree_type),
			"options": self.filters.tree_doctype,
			"fieldname": "entity",
			"fieldtype": "Link",
			"width": 200
		}]

		show_name = False
		if show_name:
			self.columns.append({
				"label": _(self.filters.tree_type + " Name"),
				"fieldname": "entity_name",
				"fieldtype": "Data",
				"width": 150,
			})

		fieldtype = self.get_value_fieldtype()
		precision = "1"

		self.columns.append({
			"label": _("Average"),
			"fieldname": "average",
			"fieldtype": fieldtype,
			"precision": precision,
			"width": 80
		})

		for end_date in self.periodic_daterange:
			period = self.get_period(end_date)
			self.columns.append({
				"label": _(period),
				"fieldname": scrub(period),
				"fieldtype": fieldtype,
				"precision": precision,
				"period_column": True,
				"width": 80
			})

	def get_data(self):
		if self.filters.tree_type == 'Air Monitor':
			self.get_entries("m.air_monitor")
			self.get_rows()

	def get_entries(self, entity_field, entity_name_field=None):
		filter_conditions = self.get_conditions()

		value_field = self.get_value_fieldname()
		entity_name_field = "{0} as entity_name, ".format(entity_name_field) if entity_name_field else ""

		self.entries = frappe.db.sql("""
			select
				{entity_field} as entity,
				{entity_name_field}
				{value_field} as value_field,
				{date_field} as date
			from `tabMonitor Reading` m
			where {date_field} between %(from_date)s and %(to_date)s
				{filter_conditions}
		""".format(
			entity_field=entity_field,
			entity_name_field=entity_name_field,
			value_field=value_field,
			date_field=self.date_field,
			filter_conditions=filter_conditions
		), self.filters, as_dict=1)

		if entity_name_field:
			for d in self.entries:
				self.entity_names.setdefault(d.entity, d.entity_name)

	def get_value_fieldname(self):
		filter_to_field = {
			"PM2.5": "pm_2_5",
			"AQI (US)": "pm_2_5",
		}
		return filter_to_field.get(self.filters.value_field, "1")

	def get_value_fieldtype(self):
		filter_to_field = {
			"PM2.5": "Float",
			"AQI (US)": "Int",
		}
		return filter_to_field.get(self.filters.value_field, "Float")

	def get_tree_doctype(self):
		return self.filters.tree_type

	def get_conditions(self):
		conditions = []

		self.date_field = "m.reading_dt"

		if self.filters.air_monitor:
			conditions.append("m.air_monitor = %(air_monitor)s")

		return "and {}".format(" and ".join(conditions)) if conditions else ""

	def get_rows(self):
		self.data = []
		self.get_periodic_data()

		total_row = frappe._dict({"entity": _("'Average'"), "sum": 0, "count": 0})
		self.data.append(total_row)

		for entity, period_data in self.entity_periodic_data.items():
			row = frappe._dict({
				"entity": entity,
				"entity_name": self.entity_names.get(entity),
				"indent": 1,
				"sum": 0,
				"count": 0,
			})

			for end_date in self.periodic_daterange:
				period = self.get_period(end_date)

				amount = flt(period_data.get(period, {}).get("sum"))
				count = cint(period_data.get(period, {}).get("count"))

				row[scrub(period)] = amount / count if count else 0
				if self.filters.value_field == "AQI (US)":
					row[scrub(period)] = calculate_aqi("PM2.5", row[scrub(period)])

				# Accumulate for entity row
				row.sum += amount
				row.count += count

				# Accumulate for total row periods
				total_row.setdefault(scrub(period + "_sum"), 0.0)
				total_row[scrub(period) + "_sum"] += amount

				total_row.setdefault(scrub(period + "_count"), 0.0)
				total_row[scrub(period) + "_count"] += count

				# Accumulate for grand total
				total_row["sum"] += amount
				total_row["count"] += count

			# Entity average
			row["average"] = row.sum / row.count if row.count else 0
			if self.filters.value_field == "AQI (US)":
				row["average"] = calculate_aqi("PM2.5", row["average"])

			self.data.append(row)

		# Total row averages
		total_row["average"] = total_row["sum"] / total_row["count"] if total_row["count"] else 0
		if self.filters.value_field == "AQI (US)":
			total_row["average"] = calculate_aqi("PM2.5", total_row["average"])

		for end_date in self.periodic_daterange:
			period = self.get_period(end_date)

			amount = flt(total_row.get(scrub(period) + "_sum"))
			count = cint(total_row.get(scrub(period) + "_count"))

			total_row[scrub(period)] = amount / count if count else 0
			if self.filters.value_field == "AQI (US)":
				total_row[scrub(period)] = calculate_aqi("PM2.5", total_row[scrub(period)])

	def get_periodic_data(self):
		self.entity_periodic_data = frappe._dict()

		for d in self.entries:
			period = self.get_period(d.get('date'))
			self.entity_periodic_data.setdefault(d.entity, frappe._dict()).setdefault(period, frappe._dict({
				"sum": 0, "count": 0,
			}))

			self.entity_periodic_data[d.entity][period]["sum"] += flt(d.value_field)
			if flt(d.value_field):
				self.entity_periodic_data[d.entity][period]["count"] += 1

	def get_period(self, posting_date):
		if self.filters.range == 'Daily':
			period = frappe.utils.format_date(posting_date)
		elif self.filters.range == 'Weekly':
			period = "W" + posting_date.strftime("%-W %Y")
		elif self.filters.range == 'Monthly':
			period = str(self.months[posting_date.month - 1]) + " " + str(posting_date.year)
		elif self.filters.range == 'Quarterly':
			period = "Q" + str(((posting_date.month - 1) // 3) + 1) + " " + str(posting_date.year)
		else:
			period = str(posting_date.year)

		return period

	def get_period_date_ranges(self):
		from dateutil.relativedelta import relativedelta, MO
		from_date, to_date = getdate(self.filters.from_date), getdate(self.filters.to_date)

		increment = {
			"Monthly": 1,
			"Quarterly": 3,
			"Half-Yearly": 6,
			"Yearly": 12
		}.get(self.filters.range, 1)

		if self.filters.range in ['Monthly', 'Quarterly', 'Yearly']:
			from_date = from_date.replace(day=1)
		elif self.filters.range == "Weekly":
			from_date = from_date + relativedelta(from_date, weekday=MO(-1))

		self.periodic_daterange = []

		while True:
			if self.filters.range == "Daily":
				period_end_date = from_date
			elif self.filters.range == "Weekly":
				period_end_date = add_days(from_date, 6)
			else:
				period_end_date = add_to_date(from_date, months=increment, days=-1)

			if period_end_date > to_date:
				period_end_date = to_date

			self.periodic_daterange.append(period_end_date)

			from_date = add_days(period_end_date, 1)
			if period_end_date >= to_date:
				break

	def get_chart_data(self):
		labels = [d.get("label") for d in self.columns if d.get("period_column")]
		self.chart = {
			"data": {
				'labels': labels,
				'datasets': []
			},
			"type": "line",
			"fieldtype": self.get_value_fieldtype(),
			"precision": 1,
		}
