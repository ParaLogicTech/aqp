# Copyright (c) 2023, ParaLogic and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.utils import get_datetime, getdate, combine_datetime, cint
from frappe.model.document import Document
from aqp.air_quality.aqi import calculate_aqi, get_aqi_category, get_daily_aggregates
from aqp.air_quality.utils import get_order_by
from datetime import timedelta
import datetime


class MonitorReading(Document):
	def validate(self):
		self.validate_duplicate()
		self.set_aqi()

	def on_update(self):
		clear_readings_cache()
		self.update_air_monitor()

	def on_trash(self):
		clear_readings_cache()
		self.update_air_monitor()

	def validate_duplicate(self):
		existing = frappe.db.get_value("Monitor Reading", {
			"air_monitor": self.air_monitor,
			"reading_dt": self.reading_dt,
			"name": ["!=", self.name]
		})

		if existing:
			frappe.throw(_("Monitor Reading for Air Monitor {0} at {1} already exists ({2})").format(
				frappe.bold(self.air_monitor),
				frappe.bold(self.get_formatted("reading_dt")),
				frappe.utils.get_link_to_form("Monitor Reading", existing)
			))

	def set_aqi(self, update=False, update_modified=True):
		self.aqi_us = calculate_aqi("PM2.5", self.pm_2_5)

		if not self.pm_2_5:
			self.aqi_category = "Not Available"
		else:
			self.aqi_category = get_aqi_category(self.aqi_us)

		if update:
			self.db_set({
				"aqi_us": self.aqi_us,
				"aqi_category": self.aqi_category,
			}, update_modified=update_modified)

	def update_air_monitor(self):
		air_monitor = frappe.get_doc("Air Monitor", self.air_monitor)
		air_monitor.set_first_last_reading(update=True)


def on_doctype_update():
	frappe.db.add_index("Monitor Reading", ["air_monitor", "reading_dt"])


def clear_readings_cache():
	cache_keys = ["latest_monitor_reading_dt"]
	for key in cache_keys:
		frappe.cache().delete_key(key)


@frappe.whitelist(allow_guest=True)
def get_latest_readings(for_datetime=None, window_minutes=60):
	from aqp.air_quality.doctype.air_monitor.air_monitor import _get_monitors
	from aqp.air_quality.doctype.monitor_region.monitor_region import _get_regions
	from aqp.air_quality.doctype.reading_aggregate.reading_aggregate import get_reading_aggregates

	if not for_datetime:
		for_datetime = get_latest_reading_dt()

	monitors = _get_monitors(filters={"first_reading_dt": ["<=", for_datetime]}, sort_by="creation", sort_order="asc")
	monitors_map = {}
	for d in monitors:
		d.has_reading = False
		monitors_map[d.name] = d

	regions = _get_regions(sort_by="lft", sort_order="asc")
	regions_map = {}
	for d in regions:
		d.has_reading = False
		regions_map[d.name] = d

	out = frappe._dict({
		"aggregates": [],
		"readings": [],
		"regions": regions,
		"monitors": monitors_map,
		"latest_reading_dt": None,
		"from_dt": None,
		"to_dt": None
	})

	if not for_datetime:
		return out

	window_minutes = cint(window_minutes)
	if window_minutes <= 0:
		frappe.throw(_("window_minutes must be a positive integer"))
	if window_minutes > 1440:
		frappe.throw(_("window_minutes cannot be greater than 1440 minutes"))

	for_datetime = get_datetime(for_datetime)
	out.to_dt = for_datetime
	out.from_dt = out.to_dt - timedelta(minutes=window_minutes)

	readings = get_monitor_readings(out.from_dt, out.to_dt, sort_order="desc")
	if readings:
		out.latest_reading_dt = readings[0].reading_dt

	aggregates = get_reading_aggregates(out.from_dt, out.to_dt, "Hourly", sort_order="desc")

	air_monitors_visited = set()
	for d in readings:
		if d.air_monitor not in air_monitors_visited:
			air_monitors_visited.add(d.air_monitor)
			out.readings.append(d)

	regions_visited = set()
	for d in aggregates:
		if d.monitor_region not in regions_visited:
			regions_visited.add(d.monitor_region)
			out.aggregates.append(d)

	for monitor in air_monitors_visited:
		if monitors_map.get(monitor):
			monitors_map[monitor].has_reading = True

	for region in regions_visited:
		if regions_map.get(region):
			regions_map[region].has_reading = True

	return out


@frappe.whitelist(allow_guest=True)
def get_latest_reading_dt():
	lastest_reading_dt = frappe.cache().get_value("latest_monitor_reading_dt", _get_latest_reading_dt)
	return get_datetime(lastest_reading_dt) if lastest_reading_dt else None


def _get_latest_reading_dt():
	latest = frappe.db.sql("""
		select reading_dt
		from `tabMonitor Reading`
		order by reading_dt desc
		limit 1
	""")

	return latest[0][0] if latest else None


def get_daily_average_readings(from_date=None, to_date=None, air_monitor=None):
	if not from_date:
		from_date = getdate()
	if not to_date:
		to_date = getdate()

	from_dt = combine_datetime(from_date, datetime.time.min)
	to_dt = combine_datetime(to_date, datetime.time.max)

	all_readings = get_monitor_readings(from_dt, to_dt, air_monitor=air_monitor, sort_order="asc")
	daily_aggregates = get_daily_aggregates(all_readings)

	return daily_aggregates


def get_monitor_readings(from_dt, to_dt, air_monitor=None, sort_order="asc"):
	if not from_dt or not to_dt:
		frappe.throw(_("From Datetime and To Datetime is required"))

	order_by = get_order_by("Monitor Reading", "reading_dt", sort_order)

	args = frappe._dict({
		"from_dt": from_dt,
		"to_dt": to_dt,
		"air_monitor": air_monitor,
	})

	monitor_condition = ""
	if isinstance(air_monitor, (list, tuple)):
		if not air_monitor:
			return []

		monitor_condition = " and r.air_monitor in %(air_monitor)s"

	elif air_monitor:
		monitor_condition = " and r.air_monitor = %(air_monitor)s"

	return frappe.db.sql(f"""
		select r.name, r.reading_dt,
			r.air_monitor,
			r.pm_2_5, r.aqi_us, r.aqi_category,
			r.temperature, r.relative_humidity, r.co2
		from `tabMonitor Reading` r
		inner join `tabAir Monitor` m on m.name = r.air_monitor
		where r.reading_dt between %(from_dt)s and %(to_dt)s
			and m.disabled = 0
			{monitor_condition}
		order by {order_by}
	""", args, as_dict=1)
