// Jitenshea functions for the 'city' page

// Note : the 'cityurl' function is in the 'app.js' file.

// List of stations with DataTables
// Store the list of station in a sessionStorage.
// The stations list item will be used for each station page (a tiny
// station-centered map)
$(document).ready(function() {
  var city = document.getElementById("citytable").getAttribute("city");
  $('#citytable').DataTable( {
    scrollY:        '80vh',
    scrollCollapse: true,
    paging:         false,
    processing: true,
    ajax: {
      url: cityurl("citytable") + "/station?limit=600"
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
    pointToLayer: function(geoJsonPoint, latlng) {
      return L.circleMarker(latlng, {radius: 5})
        .bindPopup("<ul><li><b>ID</b>: " + geoJsonPoint.properties.id
                   + "</li><li><b>Name</b>: " + geoJsonPoint.properties.name + "</li></ul>")
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
//  - is it possible to set a bbox (computed by turjs) instead of a zoom in the
//    'setView' function.
$(document).ready(function() {
  var station_map = L.map("stationMap");
  var city = document.getElementById("stationMap").getAttribute("city");
  $.get(cityurl("stationMap") + "/station?geojson=true&limit=600", function(data) {
    stationsMap(station_map, data);
  } );
} );


// Barplot of most important transactions (day before today)
$(document).ready(function() {
  var day = '2017-07-22';
  // day before today
  // var yesterday = new Date()
  // yesterday.setDate(yesterday.getDate() - 1);
  // console.log(yesterday.toISOString().substring(0, 10));
  var stations_num = 10;
  var url = cityurl("cityDailyTransactions")
      + "/daily/station?limit="+ stations_num
      + "&by=value&date=" + day;
  // var cmap = d3.interpolateRdBu();
  $.get(url, function(content) {
    // transactions values
    var values = content.data.map(function(x) {return x.value;});
    // value to compute the color according to the value [0,1]
    var cmax = content.data[0].value;
    var cmin = content.data[stations_num - 1].value;
    var cmap = values.map(function(x) {
      var scale = (x - cmin) / (cmax - cmin)
      return d3.interpolateYlGnBu(scale);
    });
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
        min: 0,
        title: {
          text: 'Number of daily transactions'
        }
      },
      plotOptions: {
        column: {
          colorByPoint: true,
          colors: cmap,
        },
      },
      series: [{
        name: "transactions",
        data: values
      }]
    } );
  } );
} );
