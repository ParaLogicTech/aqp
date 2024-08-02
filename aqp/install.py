import frappe
from frappe import _
from frappe.utils.nestedset import get_root_of


def after_install():
	create_root_monitor_region()


def create_root_monitor_region():
	root_region = get_root_of("Monitor Region")
	if root_region:
		return

	doc = frappe.new_doc("Monitor Region")
	doc.monitor_region_name = _("Global")
	doc.is_group = 1

	doc.flags.ignore_mandatory = True
	doc.insert()
