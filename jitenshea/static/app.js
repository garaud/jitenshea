// Jitenshea

var API_URL = "http://jitenshea.dev/api"

// Build the URL with a BASE_URL/<city> suffix based from a DOM element with the
// "city" attribute.
function url(dom_id) {
  return API_URL + "/" + document.getElementById(dom_id).getAttribute("city");
};
