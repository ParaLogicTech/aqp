import frappe
from frappe import _
from frappe.utils import cstr
from collections import deque


class RegionNode:
	def __init__(self, name):
		self.name = name
		self.children = []

	def level_order_traversal(self):
		levels = []
		next_level = deque([self])

		while next_level:
			current_level = next_level
			next_level = deque()
			levels.append([])

			for node in current_level:
				levels[-1].append(node.name)

				for child in node.children:
					next_level.append(child)

		return levels


def get_region_tree():
	def add_children(node):
		children = regions_by_parent.get(node.name) or []
		for ch in children:
			child_node = RegionNode(ch.name)
			node.children.append(child_node)
			add_children(child_node)

	regions = frappe.get_all("Monitor Region", fields=["name", "parent_monitor_region"])

	regions_by_parent = {}
	for d in regions:
		regions_by_parent.setdefault(cstr(d.parent_monitor_region), []).append(d)

	root_regions = regions_by_parent[""]
	if len(root_regions) == 0:
		frappe.throw(_("Root Monitor Region not found"))
	elif len(root_regions) != 1:
		frappe.throw(_("Multiple root Monitor Regions found"))

	root_node = RegionNode(root_regions[0].name)
	add_children(root_node)

	return root_node
