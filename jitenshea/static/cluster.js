// Jitenshea functions for the 'cluster' web page

function stationsMap(map, data) {
  var OSM_Mapnik = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
  });
  OSM_Mapnik.addTo(map);
  var centroid = turf.center(data);
  map.setView([centroid.geometry.coordinates[1],
               centroid.geometry.coordinates[0]], 12);
  L.geoJSON(data, {
    style: function(feature){
      return {color: d3.schemeSet1[feature.properties.cluster_id]};
    },
    pointToLayer: function(geoJsonPoint, latlng) {
      return L.circleMarker(latlng, {radius: 3})
	.bindPopup("<ul><li><b>ID</b>: " + geoJsonPoint.properties.id
		   + "</li><li><b>Name</b>: " + geoJsonPoint.properties.name + "</li>"
		   + "<li><b>Cluster id</b>: " + geoJsonPoint.properties.cluster_id + "</li></ul>")
	.on('mouseover', function(e) {
	  this.openPopup();
	})
	.on('mouseout', function(e) {
	  this.closePopup();
	});
    }
  }).addTo(map);
};

// Map with all stations with Leaflet
// TODO :
// TODO :
//  - is it possible to set a bbox (computed by turjs) instead of a zoom in the
//    'setView' function.
$(document).ready(function() {
  var station_map = L.map("clusteredStationMap");
  var city = document.getElementById("clusteredStationMap").dataset.city;
  var geostations = sessionStorage.getItem("cluster_" + city);
  if (geostations == null) {
    $.get(cityurl("clusteredStationMap") + "/clustering/stations?geojson=true", function(data) {
      console.log("stations geodata GET request in " + city);
      stationsMap(station_map, data);
      sessionStorage.setItem(city, JSON.stringify(data));
    } );
  } else {
    console.log("station geodata from sesssionStorage in " + city);
    stationsMap(station_map, JSON.parse(geostations));
  }
} );
