// Copyright (c) 2024, ParaLogic and contributors
// For license information, please see license.txt
/* eslint-disable */

frappe.query_reports["Air Quality Analytics"] = {
	filters: [
		{
			fieldname: "tree_type",
			label: __("Tree Type"),
			fieldtype: "Select",
			options: ["Monitor Region", "Air Monitor"],
			default: "Monitor Region",
			reqd: 1
		},
		{
			fieldname: "value_field",
			label: __("Value Type"),
			fieldtype: "Select",
			options: ["PM2.5", "AQI (US)"],
			default: "PM2.5",
			reqd: 1
		},
		{
			fieldname: "from_date",
			label: __("From Date"),
			fieldtype: "Date",
			reqd: 1
		},
		{
			fieldname:"to_date",
			label: __("To Date"),
			fieldtype: "Date",
			reqd: 1
		},
		{
			fieldname: "range",
			label: __("Range"),
			fieldtype: "Select",
			options: [
				{ "value": "Daily", "label": __("Daily") },
				{ "value": "Weekly", "label": __("Weekly") },
				{ "value": "Monthly", "label": __("Monthly") },
				{ "value": "Quarterly", "label": __("Quarterly") },
				{ "value": "Yearly", "label": __("Yearly") }
			],
			default: "Weekly",
			reqd: 1
		},
		{
			fieldname: "monitor_region",
			label: __("Monitor Region"),
			fieldtype: "Link",
			options: "Monitor Region",
		},
	],

	after_datatable_render: function(datatable_obj) {
		datatable_obj.rowmanager.checkRow(0, 1);
	},

	get_datatable_options(options) {
		return Object.assign(options, {
			checkboxColumn: true,
			events: {
				onCheckRow: function (data) {
					const raw_data = frappe.query_report.chart.data;

					let period_columns = [];
					$.each(frappe.query_report.columns || [], function(i, column) {
						if (column.period_column) {
							period_columns.push(i+2);
						}
					});

					const datasets = [];

					let checked_rows = frappe.query_report.datatable.rowmanager.getCheckedRows().map((i) => frappe.query_report.datatable.datamanager.getRow(i));
					for (let row of checked_rows) {
						let row_name = row[2].content;
						let row_values = period_columns.map(i => row[i].content);

						datasets.push({
							name: row_name,
							values: row_values,
						});
					}

					const new_data = {
						labels: raw_data.labels,
						datasets: datasets,
					};
					const new_options = Object.assign({}, frappe.query_report.chart_options, {data: new_data});
					frappe.query_report.render_chart(new_options);

					frappe.query_report.raw_chart_data = new_data;
				},
			},
		});
	},

	onload: function() {
		if (!frappe.query_report.get_filter_value("from_date") && !frappe.query_report.get_filter_value("to_date"))
		return frappe.call({
			method: "aqp.air_quality.doctype.monitor_reading.monitor_reading.get_latest_reading_dt",
			callback: (r) => {
				let date_obj = moment(r.message || frappe.datetime.get_today(), frappe.defaultDatetimeFormat);
				let to_date = date_obj.format(frappe.defaultDateFormat);
				let from_date = frappe.datetime.add_months(to_date, -12);

				frappe.query_report.set_filter_value("from_date", from_date);
				frappe.query_report.set_filter_value("to_date", to_date);
			}
		});
	}
};
