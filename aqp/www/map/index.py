import frappe
from aqp.air_quality.doctype.monitor_reading.monitor_reading import get_latest_readings

sitemap = 1


def get_context(context):
	context.latest_readings = get_latest_readings()
