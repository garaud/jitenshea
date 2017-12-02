// Jitenshea

var API_URL = "/api"

// Build the URL with a BASE_URL/<city> suffix based from a DOM element with the
// "city" attribute.
function cityurl(dom_id) {
  return API_URL + "/" + document.getElementById(dom_id).getAttribute("city");
};

// get the date before today in YYYY-MM-DD string format
function getYesterday() {
  var yesterday = new Date()
  yesterday.setDate(yesterday.getDate() - 1);
  // console.log(yesterday.toISOString().substring(0, 10));
  return yesterday.toISOString().substring(0, 10);
};

