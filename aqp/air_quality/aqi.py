import frappe
from frappe import _
from frappe.utils import cint, cstr, round_down, getdate
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


def calculate_aqi(pollutant_type, pollutant_value):
	if pollutant_type not in POLLUTANT_TO_AQI_POLLUTANT:
		frappe.throw(_("Pollutant Type {0} is not supported").format(pollutant_type))

	pollutant_value = round_pollutant(pollutant_type, pollutant_value)

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


def round_pollutant(pollutant_type, pollutant_value):
	if pollutant_type not in POLLUTANT_TO_AQI_POLLUTANT:
		frappe.throw(_("Pollutant Type {0} is not supported").format(pollutant_type))

	precision = POLLUTANT_PRECISION[pollutant_type]
	return round_down(pollutant_value, precision)


def get_daily_aggregates(readings):
	daily_readings = {}
	for r in readings:
		if not r.pm_2_5:
			continue

		reading_date = getdate(r.reading_dt)
		daily_readings.setdefault(reading_date, []).append(r)

	daily_aggregates = {}
	for reading_date, day_readings in daily_readings.items():
		daily_aggregates[cstr(reading_date)] = aggregate_readings(day_readings)

	return daily_aggregates


def aggregate_readings(readings, use_accumulated_values=False, agg=None):
	if not agg:
		agg = frappe._dict({
			"pm_2_5_sum": 0,
			"pm_2_5_count": 0,
			"pm_2_5_max": 0,
			"pm_2_5_min": 0,
			"pm_2_5": 0,
		})

	# PM2.5 Accumulation
	for r in readings:
		if not r.pm_2_5:
			continue

		if use_accumulated_values:
			agg.pm_2_5_sum += r.pm_2_5_sum
			agg.pm_2_5_count += r.pm_2_5_count
			agg.pm_2_5_max = max(r.pm_2_5_max, agg.pm_2_5_max)
			agg.pm_2_5_min = min(r.pm_2_5_min, agg.pm_2_5_min or 999999999)
		else:
			agg.pm_2_5_sum += r.pm_2_5
			agg.pm_2_5_count += 1
			agg.pm_2_5_max = max(r.pm_2_5, agg.pm_2_5_max)
			agg.pm_2_5_min = min(r.pm_2_5, agg.pm_2_5_min or 999999999)

	# Aggregates
	agg.pm_2_5 = agg.pm_2_5_sum / agg.pm_2_5_count if agg.pm_2_5_count else 0
	agg.pm_2_5 = round_pollutant("PM2.5", agg.pm_2_5)

	return agg
