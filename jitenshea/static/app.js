// Jitenshea

var API_URL = "/api"

// Build the URL with a BASE_URL/<city> suffix based from a DOM element with the
// "city" attribute.
function cityurl(dom_id) {
  return API_URL + "/" + document.getElementById(dom_id).getAttribute("city");
};
