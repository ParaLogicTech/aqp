# Copyright (c) 2024, ParaLogic and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils.background_jobs import get_jobs


class ReadingUpdateTool(Document):
	@frappe.whitelist()
	def enqueue_aggregate_for_regions_timerange(self):
		self.check_permission("write")
		self._validate_mandatory()

		queued_jobs = get_jobs(site=frappe.local.site, queue="long")[frappe.local.site]
		if aggregate_for_regions_timerange in queued_jobs:
			frappe.throw(_("Aggregation process is already in queue"))

		aggregate_for_regions_timerange.enqueue(
			from_dt=self.from_dt,
			to_dt=self.to_dt,
			update_hourly=not self.daily_only,
			queue="long",
		)

		frappe.msgprint(_("Aggregation process enqueued"), alert=True)


@frappe.task(timeout=60 * 60 * 6)
def aggregate_for_regions_timerange(from_dt, to_dt, update_hourly=True):
	from aqp.air_quality.doctype.reading_aggregate.reading_aggregate import aggregate_for_regions_timerange

	if update_hourly:
		aggregate_for_regions_timerange(
			from_dt, to_dt, "Hourly", update_existing=True, autocommit=True, publish_realtime=True
		)

	aggregate_for_regions_timerange(
		from_dt, to_dt, "Daily", update_existing=True, autocommit=True, publish_realtime=True
	)
