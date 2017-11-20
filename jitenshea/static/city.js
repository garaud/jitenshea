// Jitenshea functions for the 'city' page

// Note : the 'cityurl' function is in the 'app.js' file.

// List of stations with DataTables
$(document).ready(function() {
  var city = document.getElementById("citytable").getAttribute("city");
  $('#citytable').DataTable( {
    scrollY:        '80vh',
    scrollCollapse: true,
    paging:         false,
    processing: true,
    ajax: {
      url: cityurl("citytable") + "/station?limit=400"
    },
    columnDefs: [ {
      "targets": 1,
      "data": "name",
      "render": function(data, type, row, meta) {
        return '<a href="/' + city + "/" + row.id + '">' + data + '</a>';
      }
    } ],
    columns: [
      { "data": "id" },
      { "data": "name"},
      { "data": "city"},
      { "data": "nb_bikes"},
      { "data": "address"}
    ]
  } );
} );


// Map with all stations with Leaflet
// TODO :
//  - change the icon
//  - hover it with the name and nb bikes
//  - is it possible to set a bbox (computed by turjs) instead of a zoom in the
//    'setView' function.
$(document).ready(function() {
  var station_map = L.map("stationMap");
  var OSM_Mapnik = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
  });
  OSM_Mapnik.addTo(station_map);
  $.get(cityurl("stationMap") + "/station?geojson=true&limit=400", function(data) {
    // Get the centroid of all stations.
    var centroid = turf.center(data);
    station_map.setView([centroid.geometry.coordinates[1],
                         centroid.geometry.coordinates[0]], 12);
    L.geoJSON(data).addTo(station_map);
  } );
} );


// Barplot of most important transactions (day before today)
$(document).ready(function() {
  var day = '2017-07-22';
  // day before today
  // var yesterday = new Date()
  // yesterday.setDate(yesterday.getDate() - 1);
  // console.log(yesterday.toISOString().substring(0, 10));
  var url = cityurl("cityDailyTransactions") + "/daily/station?limit=10&by=value&date=" + day;
  $.get(url, function(content) {
    Highcharts.chart('cityDailyTransactions', {
      chart: {
        type: 'column'
      },
      title: {
        text: 'Daily Transactions'
      },
      xAxis: {
        categories: content.data.map(function(x) { return x.name; })
      },
      yAxis: {
        title: {
          text: 'Number of daily transactions'
        }
      },
      series: [{
        name: "transactions",
        data: content.data.map(function(x) { return x.value; })
      }]
    } );
  } );
} );
