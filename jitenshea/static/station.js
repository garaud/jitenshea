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
    $("#nbStands").append(station.nb_stands);
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

  start = toLocaleISOString(start).substring(0, 10);
  stop = toLocaleISOString(stop).substring(0, 10);

  var url = cityurl("stationTimeseries")
      + "/timeseries/station/" + station_id
      + "?start=" + start + "&stop=" + stop;
  $.get(url, function(content) {
    // just a single station
    var data = content.data[0];
    var station_name = data.name;
    var date = data.ts;
    var stands = date.map(function(t, i) {
      return [Date.parse(t), data.available_stands[i]];
    });
    var bikes = date.map(function(t, i) {
      return [Date.parse(t), data.available_bikes[i]];
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
      }]
    } );


  } );

} );


// Get the "right" date string by considering local timezone
// as date.toISOString() gives the UTC timezone
// See https://stackoverflow.com/questions/47219556/why-using-the-toisostring-method-on-a-date-object-the-hourly-value-is-an-hour
function toLocaleISOString(date) {
  function pad(number) {
    if (number < 10) {
      return '0' + number;
    }
    return number;
  }
  return date.getFullYear() +
    '-' + pad(date.getMonth() + 1) +
    '-' + pad(date.getDate()) +
    'T' + pad(date.getHours()) +
    ':' + pad(date.getMinutes()) +
    ':' + pad(date.getSeconds()) ;
}


// Predictions plot
$(document).ready(function() {
  var station_id = document.getElementById("stationPredictions").dataset.stationId;
  // Only plot seven days.
  var stop = new Date();
  var start = new Date(stop);
  start.setHours(start.getHours() - 1);
  stop.setHours(stop.getHours() + 1);
  start = toLocaleISOString(start).substring(0, 16);
  stop = toLocaleISOString(stop).substring(0, 16);

  var url = cityurl("stationPredictions")
      + "/predict/station/" + station_id
      + "?start=" + start + "&stop=" + stop + "&current=true";
  $.get(url, function(data) {
    var station_name = data[0].name;
    var nb_stands = data[0].nb_stands;
    var prediction = data.filter(function(x) {
      return x.at === '1H';
    }).map(function(x) {
        return [Date.parse(x.timestamp), x.nb_bikes];
    });
    var current = data.filter(function(x) {
      return x.at === '0';
    }).map(function(x) {
        return [Date.parse(x.timestamp), x.nb_bikes];
    });
    Highcharts.stockChart('stationPredictions', {
      // use to select the time window
      rangeSelector: {
        buttons: [{
          type: 'hour',
          count: 1,
          text: '1H'
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
        text: 'Predictions for the station ' + station_name
      },
      yAxis: {
        title: {
          text: 'Number of items'
        },
        min: 0,
        max: nb_stands
      },
      xAxis: {
        type: "datetime"
      },
      time: {
	useUTC: false
      },
      series: [{
        name: "current",
        data: current,
        color: "#66c2a5",
        tooltip: {
          valueDecimals: 1
        }
      }, {
        name: "prediction",
        data: prediction,
        color: "#fc8d62",
        tooltip: {
          valueDecimals: 1
        }}]
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
    var nb_stands = content.data[0].nb_stands;
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
        },
        min: 0,
        max: nb_stands
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
