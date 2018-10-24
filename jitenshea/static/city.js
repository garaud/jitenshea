// Jitenshea functions for the 'city' page

// Note : the 'cityurl' function is in the 'app.js' file.

// List of stations with DataTables
// Store the list of station in a sessionStorage.
// The stations list item will be used for each station page (a tiny
// station-centered map)
$(document).ready(function() {
  var element = document.getElementById("citytable");
  var city = element.dataset.city;
  $('#citytable').DataTable( {
    scrollY:        '80vh',
    scrollCollapse: true,
    paging:         false,
    processing: true,
    ajax: {
      url: cityurl("citytable") + "/infostation?limit=600"
    },
    columnDefs: [ {
      "targets": 1,
      "data": "name",
      "render": function(data, type, row, meta) {
        return '<a href="' + PREFIX + '/' + city + "/" + row.id + '">' + data + '</a>';
      }
    } ],
    columns: [
      { "data": "id" },
      { "data": "name"},
      { "data": "city"},
      { "data": "nb_stands"},
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
               centroid.geometry.coordinates[0]], 13);
  L.geoJSON(data, {
    pointToLayer: function(geoJsonPoint, latlng) {
      return L.circleMarker(latlng, {radius: 5})
        .bindPopup("<ul><li><b>ID</b>: " + geoJsonPoint.properties.id
                   + "</li><li><b>Name</b>: " + geoJsonPoint.properties.name
                   + "</li><li><b>Stands</b>: " + geoJsonPoint.properties.nb_stands
                   + "</li><li><b>Bikes</b>: " + geoJsonPoint.properties.nb_bikes
                   + "</li><li><b>Update</b>: " + geoJsonPoint.properties.timestamp + "</li></ul>")
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
  var city = document.getElementById("stationMap").dataset.city;
  var geostations = sessionStorage.getItem(city);
  if (geostations == null) {
    $.get(cityurl("stationMap") + "/station?geojson=true&limit=600", function(data) {
      console.log("stations geodata GET request in " + city);
      stationsMap(station_map, data);
      sessionStorage.setItem(city, JSON.stringify(data));
    } );
  } else {
    console.log("station geodata from sesssionStorage in " + city);
    stationsMap(station_map, JSON.parse(geostations));
  }
} );


// Barplot of most important transactions (day before today)
$(document).ready(function() {
  // day before today
  var day = getYesterday();
  var stations_num = 10;
  var element = document.getElementById("cityDailyTransactions");
  var city = element.dataset.city;
  var url = cityurl("cityDailyTransactions")
      + "/daily/station?limit="+ stations_num
      + "&by=value&date=" + day;
  // var cmap = d3.interpolateRdBu();
  $.get(url, function(content) {
    // transactions values
    if (content.length === 0) {
      console.log("WARNING: no daily transaction data for "+ day);
      return null;
    }
    var values = content.data.map(function(x) {return x.value;});
    // value to compute the color according to the value [0,1]
    var cmax = content.data[0].value;
    var cmin = content.data[stations_num - 1].value;
    // you don't want to have a too clear color for low values
    cmin = cmin - 0.3*cmin;
    var cmap = values.map(function(x) {
      var scale = (x - cmin) / (cmax - cmin)
      return d3.interpolateGnBu(scale);
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
        // Make bar clickable to the station
        series: {
          cursor: 'pointer',
          point: {
            events: {
              click: function(event) {
                window.location.href = PREFIX + '/' + city + '/' + content.data[this.index].id;
              }
            }
          }
        }
      },
      series: [{
        name: "transactions",
        data: values
      }]
    } );
  } );
} );
