(function () {
  var SUGGESTIONS = {
    company_name: ["Company Name", "Company"],
    industry: ["Industry", "Business Type"],
    company_size: ["Company size", "Company Size", "Estimated Number of Employees"],
    revenue: ["Revenue"],
    website: ["Website", "Company URL", "URL"],
    contact_name: ["Contact Name", "Name"],
    contact_first_name: ["First Name"],
    contact_last_name: ["Last Name"],
    contact_title: ["Contact Title", "Title"],
    email: ["Email Address", "Email"],
    phone: ["Phone Number", "Phone"],
    person_source: ["Person source", "Linkedin"],
    address: ["Address", "Location"],
    city: ["City"],
    state: ["State"],
    zip_code: ["Zip Code", "Zip", "Postal Code", "Postal"],
    country: ["Country"]
  };

  var TARGET_KEYS = Object.keys(SUGGESTIONS);

  function normalize(value) {
    return String(value || "").trim().toLowerCase();
  }

  function parseCsvHeaderLine(line) {
    var headers = [];
    var current = "";
    var inQuotes = false;
    var i;

    for (i = 0; i < line.length; i += 1) {
      var ch = line[i];

      if (ch === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"';
          i += 1;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (ch === "," && !inQuotes) {
        headers.push(current.trim());
        current = "";
      } else {
        current += ch;
      }
    }

    headers.push(current.trim());
    return headers.filter(function (header) { return header.length > 0; });
  }

  function firstNonEmptyLine(text) {
    var lines = String(text || "").split(/\r?\n/);
    var i;
    for (i = 0; i < lines.length; i += 1) {
      if (lines[i].trim()) {
        return lines[i];
      }
    }
    return "";
  }

  function chooseSuggestedHeader(headers, key) {
    var byNormalized = {};
    headers.forEach(function (header) {
      byNormalized[normalize(header)] = header;
    });

    var candidates = SUGGESTIONS[key] || [];
    var i;
    for (i = 0; i < candidates.length; i += 1) {
      var match = byNormalized[normalize(candidates[i])];
      if (match) {
        return match;
      }
    }
    return "";
  }

  function setSelectOptions(selectEl, headers) {
    var selected = selectEl.value;
    selectEl.innerHTML = "";

    var blank = document.createElement("option");
    blank.value = "";
    blank.textContent = "-- Not mapped --";
    selectEl.appendChild(blank);

    headers.forEach(function (header) {
      var option = document.createElement("option");
      option.value = header;
      option.textContent = header;
      selectEl.appendChild(option);
    });

    if (selected && headers.indexOf(selected) !== -1) {
      selectEl.value = selected;
    }
  }

  function applyHeaders(headers) {
    var detectedHeadersField = document.getElementById("id_detected_headers");
    if (detectedHeadersField) {
      detectedHeadersField.value = headers.join(", ");
    }

    TARGET_KEYS.forEach(function (key) {
      var selectEl = document.getElementById("id_map_" + key);
      if (!selectEl) {
        return;
      }

      var hadValue = !!selectEl.value;
      setSelectOptions(selectEl, headers);

      if (!hadValue) {
        var suggested = chooseSuggestedHeader(headers, key);
        if (suggested) {
          selectEl.value = suggested;
        }
      }
    });
  }

  function bindCsvPreview() {
    var fileInput = document.getElementById("id_csv_file");
    if (!fileInput) {
      return;
    }

    fileInput.addEventListener("change", function () {
      var file = fileInput.files && fileInput.files[0];
      if (!file) {
        applyHeaders([]);
        return;
      }

      var reader = new FileReader();
      reader.onload = function (event) {
        var firstLine = firstNonEmptyLine(event && event.target ? event.target.result : "");
        var headers = parseCsvHeaderLine(firstLine);
        applyHeaders(headers);
      };
      reader.readAsText(file);
    });
  }

  document.addEventListener("DOMContentLoaded", bindCsvPreview);
}());
