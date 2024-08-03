# Copyright (c) 2024, ParaLogic and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.utils import cstr, getdate, combine_datetime, get_datetime
from frappe.model.document import Document
from aqp.air_quality.utils import get_order_by
from aqp.air_quality.doctype.monitor_region.monitor_region import get_regions_bottom_up, get_root_region
from aqp.air_quality.doctype.monitor_reading.monitor_reading import get_monitor_readings
from aqp.air_quality.aqi import aggregate_readings, calculate_aqi, get_aqi_category
import datetime


class ReadingAggregate(Document):
	def validate(self):
		self.validate_reading_dt()
		self.validate_duplicate()
		self.set_aggregated_values()

	def validate_reading_dt(self):
		if self.reading_dt:
			self.reading_dt = truncate_reading_dt(self.reading_dt, self.timespan)

	def set_aggregated_values(self, update=False, update_modified=True):
		if not self.reading_dt or not self.timespan or not self.monitor_region:
			return

		agg = None
		if self.timespan == "Hourly":
			agg = get_hourly_aggregate_data(self.reading_dt, monitor_region=self.monitor_region)
		elif self.timespan == "Daily":
			agg = get_daily_aggregate_data(self.reading_dt, monitor_region=self.monitor_region)

		if not agg:
			return

		# Set values in document
		valid_fields = self.meta.get_fieldnames_with_value()
		to_update = {k: v for (k, v) in agg.items() if k in valid_fields}
		self.update(to_update)

		# Update DB
		if update:
			self.db_set(to_update, update_modified=update_modified)

		# AQI
		self.set_aqi(update=update, update_modified=update_modified)

	def validate_duplicate(self):
		existing = get_existing_aggregate(self.reading_dt, self.timespan, self.monitor_region,
			exclude=self.name if not self.is_new() else None)
		if existing:
			frappe.throw(_("Reading Aggregate at {0} already exists ({1})").format(
				frappe.bold(self.get_formatted("reading_dt")),
				frappe.utils.get_link_to_form("Reading Aggregate", existing)
			), exc=frappe.DuplicateEntryError)

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

	def has_data(self):
		return bool(self.pm_2_5_count)


def on_doctype_update():
	frappe.db.add_index("Reading Aggregate", ["monitor_region", "reading_dt", "timespan"])
	frappe.db.add_index("Reading Aggregate", ["reading_dt", "timespan"])


def get_hourly_aggregate_data(reading_dt, monitor_region):
	if not monitor_region:
		frappe.throw(_("Monitor Region is required"))

	reading_dt = truncate_reading_dt(reading_dt, "Hourly")
	from_dt, to_dt = get_reading_timerange(reading_dt, "Hourly")

	region_doc = frappe.get_cached_doc("Monitor Region", monitor_region)

	air_monitors = region_doc.get_direct_air_monitors()
	monitor_readings = get_monitor_readings(from_dt, to_dt, air_monitor=air_monitors)
	agg = aggregate_readings(monitor_readings, use_accumulated_values=False)

	child_regions = region_doc.get_child_regions()
	child_aggregates = get_reading_aggregates(from_dt, to_dt, "Hourly", monitor_region=child_regions)
	agg = aggregate_readings(child_aggregates, use_accumulated_values=True, agg=agg)

	return agg


def get_daily_aggregate_data(reading_dt, monitor_region):
	if not monitor_region:
		frappe.throw(_("Monitor Region is required"))

	from_dt, to_dt = get_reading_timerange(reading_dt, "Daily")

	hourly_aggregates = get_reading_aggregates(from_dt, to_dt, "Hourly", monitor_region=monitor_region)
	agg = aggregate_readings(hourly_aggregates, use_accumulated_values=True)

	return agg


def aggregate_for_regions_timerange(
	from_dt,
	to_dt,
	timespan,
	update_existing=True,
	autocommit=False,
	verbose=False,
	publish_realtime=False,
):
	reading_datetimes = get_reading_datetimes_for_timerange(from_dt, to_dt, timespan)
	count = len(reading_datetimes)

	for i, reading_dt in enumerate(reading_datetimes):
		if verbose:
			print(f"Processing {timespan} Region aggregation for timestamp {frappe.format(reading_dt)}")

		aggregate_for_regions(reading_dt, timespan, update_existing=update_existing)

		if autocommit:
			frappe.db.commit()

		if publish_realtime:
			publish_aggregation_progress(i + 1, count, timespan, reading_dt)


def publish_aggregation_progress(progress, total, timespan, reading_dt):
	finished = progress == total

	message = _("Processing {0} Region aggregation for timestamp {1} ({2}/{3})".format(
		timespan, frappe.format(reading_dt), progress, total
	))
	if finished:
		message = _("Finished: {0}").format(message)

	progress_data = {
		"progress": progress,
		"total": total,
		"message": message,
		"timespan": timespan,
	}
	frappe.publish_realtime("aggregate_for_regions_timerange_progress", progress_data,
		doctype="Reading Update Tool", docname="Reading Update Tool")


def aggregate_for_regions(reading_dt, timespan, update_existing=True):
	reading_dt = truncate_reading_dt(reading_dt, timespan)

	regions = get_regions_bottom_up()
	for monitor_region in regions:
		existing = get_existing_aggregate(reading_dt, timespan, monitor_region)
		if not existing:
			create_reading_aggregate(reading_dt, timespan, monitor_region)
		elif update_existing:
			_update_reading_aggregate(existing)


def create_reading_aggregate(reading_dt, timespan, monitor_region):
	reading_dt = truncate_reading_dt(reading_dt, timespan)

	doc = frappe.new_doc("Reading Aggregate")
	doc.monitor_region = monitor_region
	doc.reading_dt = reading_dt
	doc.timespan = timespan

	doc.validate()
	doc.flags.ignore_validate = True

	if doc.has_data():
		doc.insert()

	return doc


def update_reading_aggregate(reading_dt, timespan, monitor_region):
	reading_dt = truncate_reading_dt(reading_dt, timespan)

	existing = get_existing_aggregate(reading_dt, timespan, monitor_region)
	if existing:
		_update_reading_aggregate(existing)


def _update_reading_aggregate(name):
	doc = frappe.get_doc("Reading Aggregate", name)
	doc.set_aggregated_values(update=True)


def get_daily_reading_aggregates(from_date, to_date, monitor_region=None):
	if not monitor_region:
		monitor_region = get_root_region() or _("Global")

	from_date = getdate(from_date)
	to_date = getdate(to_date)

	from_dt = combine_datetime(from_date, datetime.time.min)
	to_dt = combine_datetime(to_date, datetime.time.max)

	reading_aggregates = get_reading_aggregates(from_dt, to_dt, "Daily", monitor_region)

	daily_aggregates = {}
	for d in reading_aggregates:
		daily_aggregates[cstr(getdate(d.reading_dt))] = d

	return daily_aggregates


def get_reading_aggregates(from_dt, to_dt, timespan, monitor_region=None, sort_order="asc"):
	if not from_dt or not to_dt:
		frappe.throw(_("From Datetime and To Datetime is required"))

	validate_timespan(timespan)

	order_by = get_order_by("Reading Aggregate", "reading_dt", sort_order)

	args = frappe._dict({
		"from_dt": from_dt,
		"to_dt": to_dt,
		"timespan": timespan,
		"monitor_region": monitor_region,
	})

	region_condition = ""
	if isinstance(monitor_region, (list, tuple)):
		if not monitor_region:
			return []

		region_condition = " and ra.monitor_region in %(monitor_region)s"

	elif monitor_region:
		region_condition = " and ra.monitor_region = %(monitor_region)s"

	return frappe.db.sql(f"""
		select ra.name, ra.timespan, ra.reading_dt,
			ra.monitor_region,
			ra.pm_2_5, ra.pm_2_5_sum, ra.pm_2_5_count, ra.pm_2_5_max, ra.pm_2_5_min,
			ra.aqi_us, ra.aqi_category
		from `tabReading Aggregate` ra
		inner join `tabMonitor Region` mr on mr.name = ra.monitor_region
		where ra.reading_dt between %(from_dt)s and %(to_dt)s
			and ra.timespan = %(timespan)s
			and mr.disabled = 0
			{region_condition}
		order by {order_by}
	""", args, as_dict=1)


def get_reading_timerange(reading_dt, timespan):
	reading_dt = truncate_reading_dt(reading_dt, timespan)

	from_dt = reading_dt
	to_dt = reading_dt

	if timespan == "Hourly":
		to_dt = reading_dt
		from_dt = to_dt - datetime.timedelta(hours=1) + datetime.datetime.resolution
	elif timespan == "Daily":
		reading_date = getdate(reading_dt)
		from_dt = combine_datetime(reading_date, datetime.time.min)
		to_dt = combine_datetime(reading_date, datetime.time.max)

	return from_dt, to_dt


def get_reading_datetimes_for_timerange(from_dt, to_dt, timespan):
	from_dt = truncate_reading_dt(from_dt, timespan)
	to_dt = truncate_reading_dt(to_dt, timespan)

	reading_datetimes = []
	current_dt = from_dt
	while current_dt <= to_dt:
		reading_datetimes.append(current_dt)
		if timespan == "Hourly":
			current_dt = current_dt + datetime.timedelta(hours=1)
		elif timespan == "Daily":
			current_dt = current_dt + datetime.timedelta(days=1)

	return reading_datetimes


def get_existing_aggregate(reading_dt, timespan, monitor_region, exclude=None):
	reading_dt = truncate_reading_dt(reading_dt, timespan)

	filters = {
		"reading_dt": reading_dt,
		"timespan": timespan,
		"monitor_region": monitor_region,
	}

	if exclude:
		filters["name"] = ["!=", exclude]

	return frappe.db.get_value("Reading Aggregate", filters)


def truncate_reading_dt(reading_dt, timespan):
	validate_timespan(timespan)

	reading_dt = get_datetime(reading_dt)
	if timespan == "Hourly":
		reading_dt = datetime.datetime(reading_dt.year, reading_dt.month, reading_dt.day, reading_dt.hour)
	elif timespan == "Daily":
		reading_dt = datetime.datetime(reading_dt.year, reading_dt.month, reading_dt.day)

	return reading_dt


def validate_timespan(timespan):
	if timespan not in ("Hourly", "Daily"):
		frappe.throw(_("Timespan must be either Hourly or Daily"))
