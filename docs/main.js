fetch("games.csv")
  .then(res => res.text())
  .then(text => {
    const container = document.getElementById("scores");

    // Split CSV into lines and remove empty lines
    const rows = text.split(/\r?\n/).filter(r => r.trim() !== "");

    if (rows.length < 2) {
      container.textContent = "No data found!";
      return;
    }

    // Get headers from the first row
    const headers = rows.shift().split(",").map(h => h.trim());

    // Create table
    const table = document.createElement("table");

    // Add header row
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    headers.forEach(h => {
      const th = document.createElement("th");
      th.textContent = h;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Add table body
    const tbody = document.createElement("tbody");
    rows.forEach(row => {
      const values = row.split(",").map(v => v.trim());
      const tr = document.createElement("tr");
      values.forEach(val => {
        const td = document.createElement("td");
        td.textContent = val;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    container.appendChild(table);
  })
  .catch(err => console.error("Error loading CSV:", err));