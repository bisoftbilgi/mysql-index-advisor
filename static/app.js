(function () {
  var box = document.getElementById("samplebox");
  var q = document.getElementById("querybox");
  if (!box || !q) return;
  box.onchange = function () {
    if (box.value) q.value = box.value;
  };
})();

