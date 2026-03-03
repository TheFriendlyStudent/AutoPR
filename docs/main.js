fetch("games.csv")
  .then(response => response.text())
  .then(text => {
    const rows = text.split("\n").slice(1); // skip headers
    const container = document.getElementById("scores");

    rows.forEach(row => {
      if (!row.trim()) return; // skip empty lines

      // Split by comma
      const cols = row.split(",");

      // Optional: parse safely in case values have commas
      const homeTeam = cols[2];
      const awayTeam = cols[3];
      const homeScore = cols[6];
      const awayScore = cols[7];
      const caption = cols[1]; // e.g., "BELT" or "FINAL"

      // Create a div for this game
      const div = document.createElement("div");
      div.textContent = `${homeTeam} (${homeScore}) vs ${awayTeam} (${awayScore}) — ${caption}`;
      container.appendChild(div);
    });
  });