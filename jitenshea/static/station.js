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
