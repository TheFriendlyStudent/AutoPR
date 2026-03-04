// main.js
fetch("games.csv")
  .then(res => res.text())
  .then(text => {
    const container = document.getElementById("scores");

    // Split CSV into lines
    const rows = text.split(/\r?\n/).filter(r => r.trim() !== "");

    rows.forEach(row => {
      // Only match lines that look like "TeamName Score - Score TeamName"
      const match = row.match(/(.+?)\s+(\d+)\s*-\s*(\d+)\s+(.+)/);
      if (match) {
        const [_, homeTeam, homeScore, awayScore, awayTeam] = match;

        const div = document.createElement("div");
        div.textContent = `${homeTeam} ${homeScore} - ${awayScore} ${awayTeam}`;
        container.appendChild(div);
      }
    });
  })
  .catch(err => console.error(err));