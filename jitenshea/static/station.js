// Jitenshea functions for the 'station' page


// Station summary
$(document).ready(function() {
  var station_id = document.getElementById("stationSummary").getAttribute("station_id");
  $.get(cityurl("stationSummary") + "/station/" + station_id, function (content) {
    var station = content.data[0];
    $("h3").append("#" + station.id + " " + station.name + " in " + station.city);
    $("#id").append(station.id);
    $("#name").append(station.name);
    $("#nbBikes").append(station.nb_bikes);
    $("#address").append(station.address);
    $("#city").append(station.city);
  } );
} );



// Timeseries plot
$(document).ready(function() {
  var station_id = document.getElementById("stationTimeseries").getAttribute("station_id");
  // Only plot two days.
  var start = '2017-07-29'
  var stop = '2017-07-31';
  // day before today
  // var yesterday = new Date()
  // yesterday.setDate(yesterday.getDate() - 1);
  // console.log(yesterday.toISOString().substring(0, 10));
  var url = cityurl("stationTimeseries") + "/timeseries/station/" + station_id
      + "?start=" + start + "&stop=" + stop;
  $.get(url, function(content) {
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
        text: 'Station ' + station_name
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
