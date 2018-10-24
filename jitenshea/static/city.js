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

// return a leaflet layer
function prepareDataMap(city, data) {
  return L.geoJSON(data, {
    pointToLayer: function(geoJsonPoint, latlng) {
      return L.circleMarker(latlng, {
        radius: 5,
        stroke: true,
        color: d3.interpolateRdYlGn(geoJsonPoint.properties.nb_bikes / geoJsonPoint.properties.nb_stands)
       })
        .bindPopup("<ul><li><b>ID</b>: " + geoJsonPoint.properties.id
                   + "</li><li><b>Name</b>: " + geoJsonPoint.properties.name
                   + "</li><li><b>Stands</b>: " + geoJsonPoint.properties.nb_stands
                   + "</li><li><b>Bikes</b>: " + geoJsonPoint.properties.nb_bikes
                   + "</li><li><b>At</b> " + geoJsonPoint.properties.timestamp + "</li></ul>")
        .on('mouseover', function(e) {
          this.openPopup();
        })
        .on('mouseout', function(e) {
          this.closePopup();
        })
      .on('click', function(e) {
        window.location.assign(city + "/" + geoJsonPoint.properties.id);
      });
    }
  });
};

// Map with all stations with Leaflet
// TODO :
//  - is it possible to set a bbox (computed by turjs) instead of a zoom in the
//    'setView' function.
$(document).ready(function() {
  var tile = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
  });
  var city = document.getElementById("stationMap").dataset.city;
  var current = getStationsLayer(city, false);
  var prediction = getStationsLayer(city, true);
  var station_map = L.map("stationMap", {
    layers: [tile, current.layer],
    center: [current.centroid.geometry.coordinates[1],
             current.centroid.geometry.coordinates[0]],
    zoom: 13
  });
  var overlaysMap = {
    "Current": current.layer,
    "Prediction 1H": prediction.layer
  };
  // current.layer.addTo(station_map);
  // prediction.layer.addTo(station_map);
  L.control.layers(null, overlaysMap).addTo(station_map);
});


function getStationsLayer(city, prediction) {
  var last_update = sessionStorage.getItem("when" + city);
  if (last_update == null) {
    last_update = new Date();
  } else {
    last_update = new Date(last_update);
  }
  var now = new Date();
  // Force a new request if the last update > 5 min.
  var force_request = (now - last_update) / (60 * 1000.) > 5;
  if (prediction) {
    var prefixurl = "/predict/station";
  } else {
    var prefixurl = "/station";
  }
  var key = "map" + prefixurl + "/" + city;
  var geostations = sessionStorage.getItem(key);
  force_request = true;
  if (force_request || (geostations == null)) {
    $.get(cityurl("stationMap") + prefixurl + "?geojson=true&limit=600", function(data) {
      console.log("stations geodata GET request key " + key);
      sessionStorage.setItem("when" + city, now.toISOString());
      sessionStorage.setItem(key, JSON.stringify(data));
    } );
  }
  console.log("station geodata from sesssionStorage key " + key);
  var data = JSON.parse(sessionStorage.getItem(key));
  console.log(data);
  return {"layer": prepareDataMap(city, data),
          "centroid": turf.center(data) };
};



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
