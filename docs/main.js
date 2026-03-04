fetch("games.csv")
  .then(response => response.text())
  .then(text => {
    const rows = text.split("\n").slice(1); // skip headers
    const container = document.getElementById("scores");

    rows.forEach(row => {
      if (!row.trim()) return; // skip empty lines

      const cols = row.split(",");

      const homeTeam = cols[2];
      const awayTeam = cols[3];
      const homeScore = cols[6];
      const awayScore = cols[7];

      const div = document.createElement("div");
      div.textContent = `${homeTeam} ${homeScore} - ${awayScore} ${awayTeam}`;
      container.appendChild(div);
    });
  });