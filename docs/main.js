fetch("scores.csv")
  .then(response => response.text())
  .then(text => {
    const rows = text.split("\n").slice(1); // skip headers
    const container = document.getElementById("scores");

    rows.forEach(row => {
      const cols = row.split(",");
      const div = document.createElement("div");
      div.textContent = `${cols[2]} (${cols[6]}) vs ${cols[3]} (${cols[7]})`;
      container.appendChild(div);
    });
  });