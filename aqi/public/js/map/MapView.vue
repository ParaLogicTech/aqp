<template>
	<l-map :zoom="zoom" :center="center">
		<l-tile-layer :url="url" :attribution="attribution"></l-tile-layer>
<!--
		<l-marker :lat-lng="markerLatLng"></l-marker>
-->
	</l-map>
</template>

<script>
import L from 'leaflet';
import { LMap, LTileLayer, LMarker } from 'vue2-leaflet';

export default {
	name: "MapView",

	components: {
		LMap,
		LTileLayer,
		LMarker,
	},

	data () {
		return {
			url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
			zoom: 5.5,
			center: new L.LatLng(30.0, 70.0),
			attribution: "Testing"
		}
	},
};
</script>
