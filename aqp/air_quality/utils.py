import frappe
from frappe import _
from frappe.utils import cstr


def get_order_by(doctype, sort_by, sort_order, fields):
	if not sort_by:
		return None

	if not sort_order:
		sort_order = "asc"

	sort_order = cstr(sort_order).lower()
	if sort_order not in ("asc", "desc"):
		frappe.throw(_("sort_order must be either 'asc' or 'desc'"))

	if not fields:
		meta = frappe.get_meta(doctype)
		fields = [d.fieldname for d in meta.fields] + ["creation", "modified"]

	if sort_by not in fields:
		frappe.throw(_("sort_by must be one of {0}").format(
			", ".join([f"'{f}'" for f in fields])
		), exc=frappe.db.InvalidColumnName)

	return f"{sort_by} {sort_order}"
