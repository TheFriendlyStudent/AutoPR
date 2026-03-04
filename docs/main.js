fetch("games.csv")
  .then(res => res.text())
  .then(text => {
    const container = document.getElementById("scores");
    container.innerHTML = "";

    const rows = text.split(/\r?\n/).filter(r => r.trim() !== "");
    const headers = rows.shift().split(",").map(h => h.trim());

    // Match your REAL column names (lowercase + underscores)
    const homeTeamIdx = headers.indexOf("home_team");
    const homeScoreIdx = headers.indexOf("home_score");
    const awayTeamIdx = headers.indexOf("away_team");
    const awayScoreIdx = headers.indexOf("away_score");
    const homeRecordIdx = headers.indexOf("home_record");
    const awayRecordIdx = headers.indexOf("away_record");

    // Safety check
    if (
      homeTeamIdx === -1 ||
      homeScoreIdx === -1 ||
      awayTeamIdx === -1 ||
      awayScoreIdx === -1
    ) {
      container.textContent = "Column names not found. Check CSV header.";
      return;
    }

    rows.forEach(row => {
      const values = row.split(",").map(v => v.trim());

      const homeTeam = values[homeTeamIdx];
      const awayTeam = values[awayTeamIdx];
      const homeScore = parseInt(values[homeScoreIdx]);
      const awayScore = parseInt(values[awayScoreIdx]);
      const homeRecord = values[homeRecordIdx] || "";
      const awayRecord = values[awayRecordIdx] || "";

      const gameRow = document.createElement("div");
      gameRow.className = "game-row";

      let homeColor = "black";
      let awayColor = "black";

      if (homeScore > awayScore) {
        homeColor = "green";
        awayColor = "red";
      } else if (awayScore > homeScore) {
        homeColor = "red";
        awayColor = "green";
      } else {
        homeColor = awayColor = "gray";
      }

      gameRow.innerHTML = `
        <div class="team">
          <div class="team-name" style="color:${homeColor}">${homeTeam}</div>
          <div class="team-record">${homeRecord}</div>
        </div>

        <div class="team-score" style="color:${homeColor}">
          ${homeScore}
        </div>

        <div class="center-info">Final</div>

        <div class="team-score" style="color:${awayColor}">
          ${awayScore}
        </div>

        <div class="team">
          <div class="team-name" style="color:${awayColor}">${awayTeam}</div>
          <div class="team-record">${awayRecord}</div>
        </div>
      `;

      container.appendChild(gameRow);
    });
  })
  .catch(err => console.error(err));