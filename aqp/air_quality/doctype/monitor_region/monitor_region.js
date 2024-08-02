// Copyright (c) 2024, ParaLogic and contributors
// For license information, please see license.txt

frappe.ui.form.on('Monitor Region', {
	before_load: function(frm) {
		frm.events.update_timezone_options(frm);
	},

	update_timezone_options(frm) {
		let update_tz_select = function (user_language) {
			frm.set_df_property("timezone", "options", [""].concat(frappe.all_timezones));
		};

		if (!frappe.all_timezones) {
			frappe.call({
				method: "frappe.core.doctype.user.user.get_timezones",
				callback: function (r) {
					frappe.all_timezones = r.message.timezones;
					update_tz_select();
				},
			});
		} else {
			update_tz_select();
		}
	}
});
