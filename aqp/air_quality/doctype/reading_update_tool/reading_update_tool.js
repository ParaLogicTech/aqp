// Copyright (c) 2024, ParaLogic and contributors
// For license information, please see license.txt

frappe.ui.form.on('Reading Update Tool', {
	refresh(frm) {
		frm.disable_save();
		frm.events.setup_progressbar(frm);
	},

	update_reading_aggregates(frm) {
		return frm.call({
			method: "enqueue_aggregate_for_regions_timerange",
			doc: frm.doc,
			callback() {
				frm.dashboard.progress_area.body.empty();
				frm.dashboard.progress_area.hide();
				frm.dashboard._progress_map = {};
			}
		});
	},

	setup_progressbar(frm) {
		frappe.realtime.on("aggregate_for_regions_timerange_progress", (progress_data) => {
			if (progress_data) {
				frm.dashboard.show_progress(__("Processing {0} Aggregates", [progress_data.timespan]),
					cint(progress_data.total) ? cint(progress_data.progress) / cint(progress_data.total) * 100 : 0,
					progress_data.message || ""
				);
			}
		});
	},
});
