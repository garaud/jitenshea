// Jitenshea functions for the 'station' page


// Station summary
// TODO: handle the case when the ID does not exist with an error func callback
// in the GET jQuery
$(document).ready(function() {
  var station_id = document.getElementById("stationSummary").dataset.stationId;
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



// Timeseries plot
$(document).ready(function() {
  var station_id = document.getElementById("stationTimeseries").dataset.stationId;
  // Only plot seven days.
  var stop = new Date();
  var start = new Date(stop);
  stop.setDate(stop.getDate() + 1);
  start.setDate(start.getDate() - 7);

  start = start.toISOString().substring(0, 10);
  stop = stop.toISOString().substring(0, 10);

  var url = cityurl("stationTimeseries")
      + "/timeseries/station/" + station_id
      + "?start=" + start + "&stop=" + stop;
  $.get(url, function(content) {
    var station_name = content.timeseries[0].name;
    var date = content.timeseries[0].ts;
    var prediction_date = content.predictions[0].ts;
    var stands = date.map(function(t, i) {
      return [Date.parse(t), content.timeseries[0].available_stands[i]];
    });
    var bikes = date.map(function(t, i) {
      return [Date.parse(t), content.timeseries[0].available_bikes[i]];
    });
    var predicted_stands = prediction_date.map(function(t, i) {
      return [Date.parse(t), content.predictions[0].predicted_stands[i]];
    });
    var predicted_bikes = prediction_date.map(function(t, i) {
      return [Date.parse(t), content.predictions[0].predicted_bikes[i]];
    });
    Highcharts.stockChart('stationTimeseries', {
      // use to select the time window
      rangeSelector: {
        buttons: [{
          type: 'day',
          count: 1,
          text: '1D'
        }, {
          type: 'day',
          count: 2,
          text: '2D'
        }, {
          type: 'all',
          count: 1,
          text: 'All'
        }],
        selected: 0,
        inputEnabled: false
      },
      legend:{
	align: 'center',
	verticalAlign: 'top',
	enabled: true
      },
      title: {
        text: 'Timeseries for the station ' + station_name
      },
      yAxis: {
        title: {
          text: 'Number of items'
        }
      },
      xAxis: {
        type: "datetime"
      },
      time: {
	useUTC: false
      },
      series: [{
        name: "stands",
        data: stands,
        tooltip: {
          valueDecimals: 1
        }
      }, {
        name: "bikes",
        data: bikes,
        tooltip: {
          valueDecimals: 1
        }
      }, {
        name: "predicted stands",
        data: predicted_stands,
        tooltip: {
          valueDecimals: 1
        }
      }, {
        name: "predicted bikes",
        data: predicted_bikes,
        tooltip: {
          valueDecimals: 1
        }
      }]
    } );


  } );

} );


// Daily transactions plot
$(document).ready(function() {
  var station_id = document.getElementById("stationDailyTransactions").dataset.stationId;
  // Only plot seven days.
  var window = 7;
  // day before today
  var day = getYesterday();
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
  var station_id = document.getElementById("stationProfileDay").dataset.stationId;
  // day before today
  var day = getYesterday();
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
  var station_id = document.getElementById("stationProfileWeek").dataset.stationId;
  // day before today
  var day = getYesterday();
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
