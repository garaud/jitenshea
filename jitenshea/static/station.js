// Jitenshea functions for the 'station' page


// Station summary
// TODO: handle the case when the ID does not exist with an error func callback
// in the GET jQuery
$(document).ready(function() {
  var station_id = document.getElementById("stationSummary").getAttribute("station_id");
  $.get(cityurl("stationSummary") + "/station/" + station_id, function (content) {
    var station = content.data[0];
    $("#titlePanel").append("#" + station.id + " " + station.name + " in " + station.city);
    $("#id").append(station.id);
    $("#name").append(station.name);
    $("#nbBikes").append(station.nb_bikes);
    $("#address").append(station.address);
    $("#city").append(station.city);
  } );
} );


// Get the station ID from the URL /<city>/ID
function stationIdFromURL() {
  var path = window.location.pathname;
  var elements = path.split('/')
  return parseInt(elements[elements.length -1]);
}

// Map centered to the station
// TODO: handle the case when the station ID does not exist
function stationsMap(map, data) {
  var OSM_Mapnik = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
  });
  OSM_Mapnik.addTo(map);
  // The center of the map is the station coords
  var station_id = stationIdFromURL();
  var station = data.features.filter(feature => feature.properties.id == station_id);
  map.setView([station[0].geometry.coordinates[1],
               station[0].geometry.coordinates[0]], 16);
  L.geoJSON(data, {
    pointToLayer: function(geoJsonPoint, latlng) {
      var marker = null;
      if (geoJsonPoint.properties.id == station_id) {
        marker = L.marker(latlng);
      } else {
        marker = L.circleMarker(latlng, {radius: 5});
      }
      // Popup with ID and name.
      marker.bindPopup("<ul><li><b>ID</b>: " + geoJsonPoint.properties.id
                       + "</li><li><b>Name</b>: " + geoJsonPoint.properties.name + "</li></ul>")
        .on('mouseover', function(e) {
          this.openPopup();
        })
        .on('mouseout', function(e) {
          this.closePopup();
        });
      return marker;
    }
  }).addTo(map);
};


// Map centered to the station with Leaflet
$(document).ready(function() {
  var station_map = L.map("stationMap");
  var city = document.getElementById("stationMap").getAttribute("city");
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



// Timeseries plot
$(document).ready(function() {
  var station_id = document.getElementById("stationTimeseries").getAttribute("station_id");
  // Only plot two days.
  var start = '2017-07-29'
  var stop = '2017-07-31';
  // day before today
  var yesterday = new Date()
  yesterday.setDate(yesterday.getDate() - 1);
  console.log(yesterday.toISOString().substring(0, 10));
  var url = cityurl("stationTimeseries") + "/timeseries/station/" + station_id
      + "?start=" + start + "&stop=" + stop;
  $.get(url, function(content) {
    console.log(content);
    var station_name = content.data[0].name;
    var date = content.data[0].ts;
    var stands = date.map(function(t, i) {
      return [Date.parse(t), content.data[0].available_stand[i]];
    });
    var bikes = date.map(function(t, i) {
      return [Date.parse(t), content.data[0].available_bike[i]];
    });
    Highcharts.chart('stationTimeseries', {
      title: {
        text: 'Timeseries for the station ' + station_name
      },
      yAxis: {
        title: {
          text: 'Available bikes and stands'
        }
      },
      xAxis: {
        type: "datetime"
      },
      series: [{
        "name": "stands",
        "data": stands
      }, {
        "name": "bikes",
        "data": bikes
      }]
    } );
  } );
} );


// Daily transactions plot
$(document).ready(function() {
  var station_id = document.getElementById("stationDailyTransactions").getAttribute("station_id");
  // Only plot seven days.
  var day = '2017-07-29'
  var window = 7;
  // day before today
  // var yesterday = new Date()
  // yesterday.setDate(yesterday.getDate() - 1);
  // console.log(yesterday.toISOString().substring(0, 10));
  var url = cityurl("stationDailyTransactions") + "/daily/station/" + station_id
      + "?date=" + day + "&window=" + window;
  $.get(url, function(content) {
    var station_name = content.data[0].name;
    var date = content.data[0].date;
    var data = date.map(function(t, i) {
      return [Date.parse(t), content.data[0].value[i]];
    });
    Highcharts.chart('stationDailyTransactions', {
      chart: {
        type: 'column'
      },
      title: {
        text: 'Daily transactions for the station ' + station_name
      },
      yAxis: {
        title: {
          text: 'Daily Transactions'
        }
      },
      xAxis: {
        type: "datetime",
      },
      series: [{
        "name": "transactions",
        // "data": content.data[0].value
        "data": data
      }]
    } );
  } );
} );


// Day profile
$(document).ready(function() {
  var station_id = document.getElementById("stationProfileDay").getAttribute("station_id");
  var day = '2017-07-29'
  // day before today
  // var yesterday = new Date()
  // yesterday.setDate(yesterday.getDate() - 1);
  // console.log(yesterday.toISOString().substring(0, 10));
  var url = cityurl("stationProfileDay") + "/profile/hourly/station/" + station_id
      + "?date=" + day;
  $.get(url, function(content) {
    var station_name = content.data[0].name;
    var data = content.data[0].mean;
    Highcharts.chart('stationProfileDay', {
      chart: {
        type: 'column'
      },
      title: {
        text: 'Day profile for the station ' + station_name
      },
      yAxis: {
        title: {
          text: 'Transactions mean'
        }
      },
      xAxis: {
        categories: [...Array(24).keys()].map(function(x) { return x.toString().padStart(2, '0') + 'h'; })
      },
      tooltip: {
        pointFormat: "Mean: {point.y:.2f}"
      },
      series: [{
        "name": "Transaction mean",
        "data": data,
        "color": "#90ed7d"
      }]
    } );
  } );
} );


// Week profile
$(document).ready(function() {
  var station_id = document.getElementById("stationProfileWeek").getAttribute("station_id");
  var day = '2017-07-29'
  // day before today
  // var yesterday = new Date()
  // yesterday.setDate(yesterday.getDate() - 1);
  // console.log(yesterday.toISOString().substring(0, 10));
  var url = cityurl("stationProfileWeek") + "/profile/daily/station/" + station_id
      + "?date=" + day;
  $.get(url, function(content) {
    var station_name = content.data[0].name;
    var data = content.data[0].mean;
    Highcharts.chart('stationProfileWeek', {
      chart: {
        type: 'column'
      },
      title: {
        text: 'Week profile for the station ' + station_name
      },
      yAxis: {
        title: {
          text: 'Transactions mean'
        }
      },
      xAxis: {
        categories: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
      },
      tooltip: {
        pointFormat: "Mean: {point.y:.2f}"
      },
      series: [{
        "name": "Transaction mean",
        "data": data,
        "color": "#f45b5b"
      }]
    } );
  } );
} );
