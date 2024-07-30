# Copyright (c) 2023, ParaLogic and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.utils import get_datetime, round_down, getdate, combine_datetime, cint, flt, cstr
from frappe.model.document import Document
from datetime import timedelta
import datetime
import aqi


POLLUTANT_TO_AQI_POLLUTANT = {
	"PM2.5": aqi.POLLUTANT_PM25,
	"PM10": aqi.POLLUTANT_PM10,
	"O3": aqi.POLLUTANT_O3_8H,
	"SO2": aqi.POLLUTANT_SO2_1H,
	"NO2": aqi.POLLUTANT_NO2_1H,
	"CO": aqi.POLLUTANT_CO_8H,
}

POLLUTANT_PRECISION = {
	"PM2.5": 1,
	"PM10": 0,
	"O3": 3,
	"SO2": 0,
	"NO2": 0,
	"CO": 1,
}

POLLUTANT_MAX_RANGE = {
	"PM2.5": [500.5, 500],
	"PM10": [605, 500],
	"O3": [0.375, 300],
	"SO2": [1005, 500],
	"NO2": [2050, 500],
	"CO": [50.5, 500],
}


class MonitorReading(Document):
	def validate(self):
		self.validate_duplicate()
		self.calculate_aqi()
		self.determine_aqi_category()

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

	def calculate_aqi(self):
		self.aqi_us = calculate_aqi("PM2.5", self.pm_2_5)

	def determine_aqi_category(self):
		if not self.pm_2_5:
			self.aqi_category = "Not Available"
		else:
			self.aqi_category = get_aqi_category(self.aqi_us)

	def update_air_monitor(self):
		air_monitor = frappe.get_doc("Air Monitor", self.air_monitor)
		air_monitor.set_first_last_reading(update=True)


def clear_readings_cache():
	cache_keys = ["latest_monitor_reading_dt"]
	for key in cache_keys:
		frappe.cache().delete_key(key)


def calculate_aqi(pollutant_type, pollutant_value):
	if pollutant_type not in POLLUTANT_TO_AQI_POLLUTANT:
		frappe.throw(_("Pollutant Type {0} is not supported").format(pollutant_type))

	precision = POLLUTANT_PRECISION[pollutant_type]
	pollutant_value = round_down(pollutant_value, precision)

	max_limit, limit_aqi = POLLUTANT_MAX_RANGE[pollutant_type]
	if pollutant_value >= max_limit:
		return limit_aqi

	return cint(aqi.to_iaqi(
		POLLUTANT_TO_AQI_POLLUTANT[pollutant_type],
		pollutant_value,
		algo=aqi.ALGO_EPA,
	))


def get_aqi_category(aqi_value):
	aqi_value = cint(aqi_value)
	if aqi_value <= 50:
		return "Good"
	elif aqi_value <= 100:
		return "Moderate"
	elif aqi_value <= 150:
		return "Unhealthy for Sensitive Groups"
	elif aqi_value <= 200:
		return "Unhealthy"
	elif aqi_value <= 300:
		return "Very Unhealthy"
	else:
		return "Hazardous"


@frappe.whitelist(allow_guest=True)
def get_latest_readings(for_datetime=None, window_minutes=60):
	from aqp.air_quality.doctype.air_monitor.air_monitor import _get_monitors

	if not for_datetime:
		for_datetime = get_latest_reading_dt()

	monitors = _get_monitors(filters={"first_reading_dt": ["<=", for_datetime]})
	monitors_map = {}
	for d in monitors:
		d.has_reading = False
		monitors_map[d.name] = d

	out = frappe._dict({
		"readings": [],
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

	readings = get_monitor_readings(out.from_dt, out.to_dt, order_by="desc")
	if readings:
		out.latest_reading_dt = readings[0].reading_dt

	air_monitors_visited = set()
	for d in readings:
		if d.air_monitor not in air_monitors_visited:
			air_monitors_visited.add(d.air_monitor)
			out.readings.append(d)

	for monitor in air_monitors_visited:
		if monitors_map.get(monitor):
			monitors_map[monitor].has_reading = True

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


@frappe.whitelist(allow_guest=True)
def get_daily_average_readings(from_date=None, to_date=None, air_monitor=None):
	if not from_date:
		from_date = getdate()
	if not to_date:
		to_date = getdate()

	from_dt = combine_datetime(from_date, datetime.time.min)
	to_dt = combine_datetime(to_date, datetime.time.max)

	all_readings = get_monitor_readings(from_dt, to_dt, air_monitor=air_monitor, order_by="asc")
	daily_averages = calculate_daily_average_readings(all_readings)

	return daily_averages


def calculate_daily_average_readings(readings):
	date_dict_zero = frappe._dict({
		"pm_2_5_sum": 0,
		"pm_2_5_count": 0,
		"pm_2_5_max": 0,
		"pm_2_5_min": 0,
		"pm_2_5_mean": 0,
	})

	daily_averages = {}
	for r in readings:
		if not r.pm_2_5:
			continue

		reading_date_obj = getdate(r.reading_dt)
		reading_date = cstr(reading_date_obj)
		if reading_date not in daily_averages:
			daily_averages[reading_date] = date_dict_zero.copy()

		date_dict = daily_averages[reading_date]
		date_dict.pm_2_5_sum += r.pm_2_5
		date_dict.pm_2_5_count += 1
		date_dict.pm_2_5_max = max(r.pm_2_5, date_dict.pm_2_5_max)
		date_dict.pm_2_5_min = min(r.pm_2_5, date_dict.pm_2_5_min or 999999999)

	for date_dict in daily_averages.values():
		date_dict.pm_2_5_mean = flt(date_dict.pm_2_5_sum / date_dict.pm_2_5_count, 9) if date_dict.pm_2_5_count else 0
		date_dict.aqi_us = calculate_aqi("PM2.5", date_dict.pm_2_5_mean)
		date_dict.aqi_category = get_aqi_category(date_dict.aqi_us)

	return daily_averages


def get_monitor_readings(from_dt, to_dt, air_monitor=None, order_by="asc"):
	if not from_dt or not to_dt:
		frappe.throw(_("From Datetime and To Datetime is required"))

	order_by = cstr(order_by).lower() or "asc"
	if order_by not in ("asc", "desc"):
		frappe.throw(_("Invalid Order By {0}").format(order_by))

	args = frappe._dict({
		"from_dt": from_dt,
		"to_dt": to_dt,
		"air_monitor": air_monitor,
	})

	monitor_condition = ""
	if air_monitor:
		monitor_condition = " and r.air_monitor = %(air_monitor)s"

	return frappe.db.sql(f"""
		select r.name, r.reading_dt,
			r.air_monitor,
			r.pm_2_5, r.aqi_us,
			r.temperature, r.relative_humidity, r.co2
		from `tabMonitor Reading` r
		inner join `tabAir Monitor` m on m.name = r.air_monitor
		where r.reading_dt between %(from_dt)s and %(to_dt)s and m.disabled = 0
			{monitor_condition}
		order by r.reading_dt {order_by}
	""", args, as_dict=1)
