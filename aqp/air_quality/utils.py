import frappe
from frappe import _
from frappe.utils import cstr


def get_order_by(doctype, sort_by, sort_order, fields=None):
	if not sort_by:
		return None

	if not sort_order:
		sort_order = "asc"

	sort_order = cstr(sort_order).lower()
	if sort_order not in ("asc", "desc"):
		frappe.throw(_("sort_order must be either 'asc' or 'desc'"))

	if not fields:
		meta = frappe.get_meta(doctype)
		fields = meta.get_fieldnames_with_value() + ["creation", "modified"]

	if sort_by not in fields:
		frappe.throw(_("sort_by {0} is not permitted").format(
			sort_by
		), exc=frappe.db.InvalidColumnName)

	return f"{sort_by} {sort_order}"
