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


// Timeseries plot
$(document).ready(function() {
  var url = cityurl("clusterCentroids") + "/clustering/centroids";
  $.get(url, function(content) {

    var cluster0 = content.data[0].hour.map(function(t, i) {
      return [t, content.data[0].values[i]];
    });
    var cluster1 = content.data[1].hour.map(function(t, i) {
      return [t, content.data[1].values[i]];
    });
    var cluster2 = content.data[2].hour.map(function(t, i) {
      return [t, content.data[2].values[i]];
    });
    var cluster3 = content.data[3].hour.map(function(t, i) {
      return [t, content.data[3].values[i]];
    });

    Highcharts.chart('clusterCentroids', {
      title: {
        text: 'Cluster centroid definitions'
      },
      yAxis: {
        title: {
          text: 'Available bike percentage'
        }
      },
      xAxis: {
        type: "Hour of the day"
      },
      colors: d3.schemeSet1,
      series: [{
        name: "cluster 0",
        data: cluster0,
        tooltip: {
          valueDecimals: 2
        }
      },{
        name: "cluster 1",
        data: cluster1,
        tooltip: {
          valueDecimals: 2
        }
      },{
        name: "cluster 2",
        data: cluster2,
        tooltip: {
          valueDecimals: 2
        }
      },{
        name: "cluster 3",
        data: cluster3,
        tooltip: {
          valueDecimals: 2
        }
      }]
    } );
  } );
} );
