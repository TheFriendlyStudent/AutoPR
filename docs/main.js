fetch("games.csv")
  .then(res => res.text())
  .then(text => {
    const container = document.getElementById("scores");
    const rows = text.split(/\r?\n/).filter(r => r.trim() !== "");

    const headers = rows.shift().split(",").map(h => h.trim());

    const homeTeamIdx = headers.indexOf("home_team");
    const homeScoreIdx = headers.indexOf("home_score");
    const awayTeamIdx = headers.indexOf("away_team");
    const awayScoreIdx = headers.indexOf("away_score");
    const homeRecord = values[headers.indexOf("home_record")] || "";
    const awayRecord = values[headers.indexOf("away_record")] || "";

    rows.forEach(row => {
      const values = row.split(",").map(v => v.trim());
      const homeTeam = values[homeTeamIdx];
      const homeScore = parseInt(values[homeScoreIdx]);
      const awayTeam = values[awayTeamIdx];
      const awayScore = parseInt(values[awayScoreIdx]);

      const gameRow = document.createElement("div");
      gameRow.className = "game-row";

      // Decide colors
      let homeColor = "black";
      let awayColor = "black";
      if (homeScore > awayScore) homeColor = "green", awayColor = "red";
      else if (awayScore > homeScore) homeColor = "green", homeColor = "red";
      else homeColor = awayColor = "gray";

      // Home team
      const homeDiv = document.createElement("div");
      homeDiv.className = "team";
      homeDiv.innerHTML = `<span class="team-name" style="color:${homeColor}">${homeTeam}</span>      
      <div class="team-name" style="color:${homeColor}">${homeTeam}</div>
      <div class="team-record">${homeRecord}</div>
      `;

      const homeScoreSpan = document.createElement("span");
      homeScoreSpan.className = "team-score";
      homeScoreSpan.textContent = homeScore;
      homeScoreSpan.style.color = homeColor;

      // Away team
      const awayDiv = document.createElement("div");
      awayDiv.className = "team";
      awayDiv.innerHTML = `<span class="team-name" style="color:${awayColor}">${awayTeam}</span>  
      <div class="team-name" style="color:${awayColor}">${awayTeam}</div>
  <div class="team-record">${awayRecord}</div>
`;

      const awayScoreSpan = document.createElement("span");
      awayScoreSpan.className = "team-score";
      awayScoreSpan.textContent = awayScore;
      awayScoreSpan.style.color = awayColor;

      // Center info (optional)
      const centerDiv = document.createElement("div");
      centerDiv.className = "center-info";
      centerDiv.textContent = "Final"; // replace with dynamic info if available

      gameRow.appendChild(homeDiv);
      gameRow.appendChild(homeScoreSpan);
      gameRow.appendChild(centerDiv);
      gameRow.appendChild(awayScoreSpan);
      gameRow.appendChild(awayDiv);

      container.appendChild(gameRow);
    });
  })
  .catch(err => console.error(err));