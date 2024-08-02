# Copyright (c) 2024, ParaLogic and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.utils import cint
from frappe.utils.nestedset import NestedSet, get_root_of
from aqp.air_quality.doctype.air_monitor.air_monitor import _get_monitors
from aqp.air_quality.utils import get_order_by
from aqp.air_quality.doctype.monitor_region.region_tree import get_region_tree


class MonitorRegion(NestedSet):
	def on_update(self):
		super().on_update()
		self.validate_one_root()

	def get_direct_air_monitors(self):
		return _get_monitors(filters={"monitor_region": self.name}, pluck="name")

	def get_all_air_monitors(self):
		return _get_monitors(filters={"monitor_region": ["subtree of", self.name]}, pluck="name")

	def get_child_regions(self):
		return _get_regions(filters={"parent_monitor_region": self.name}, pluck="name")


@frappe.whitelist()
def get_regions(filters=None, limit_start=0, limit_page_length=20, sort_by="lft", sort_order="asc"):
	regions = _get_regions(filters, limit_start, limit_page_length, sort_by, sort_order)

	return frappe._dict({
		"data": regions,
		"pagination": frappe._dict({
			"count": len(regions),
			"total_count": frappe.db.count("Monitor Region", filters),
			"limit_start": cint(limit_start),
			"limit_page_length": cint(limit_page_length),
		}),
	})


def _get_regions(filters=None, limit_start=None, limit_page_length=None, sort_by=None, sort_order=None, pluck=None):
	fields = [
		"name", "monitor_region_name", "parent_monitor_region",
		"type", "timezone",
		"lft", "rgt",
		"creation", "modified",
	]

	if not filters:
		filters = {}

	if isinstance(filters, dict):
		filters["disabled"] = 0
	elif isinstance(filters, list):
		filters.append(["Monitor Region", "disabled", "=", 0])

	return frappe.get_all(
		"Monitor Region",
		fields=fields,
		filters=filters,
		limit_start=limit_start,
		limit_page_length=limit_page_length,
		order_by=get_order_by("Monitor Region", sort_by, sort_order, fields),
		pluck=pluck,
	)


def get_regions_bottom_up():
	def generator():
		region_tree = get_region_tree()

		levels = region_tree.level_order_traversal()
		bottom_up_list = []
		for level in reversed(levels):
			for name in level:
				bottom_up_list.append(name)

		return bottom_up_list

	return frappe.local_cache("get_regions_bottom_up", "", generator)


def get_root_region():
	return get_root_of("Monitor Region")
