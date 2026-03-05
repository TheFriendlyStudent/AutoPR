document.addEventListener("DOMContentLoaded", () => {

  // ===== TAB SWITCHING =====
  const tabs = document.querySelectorAll(".tab");
  const sections = {
    scores: document.getElementById("scores"),
    livestreams: document.getElementById("livestreams")
  };

  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      const tabName = tab.textContent.toLowerCase();
      Object.values(sections).forEach(sec => sec.style.display = "none");
      if (sections[tabName]) sections[tabName].style.display = "block";
      tabs.forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
    });
  });

  // Default tab
  sections.scores.style.display = "block";

  // ===== FETCH SCORES FROM CSV =====
  fetch("games.csv")
    .then(res => res.text())
    .then(text => {
      const container = document.getElementById("scores");
      container.innerHTML = "";

      const ticker = document.getElementById("scoreTicker");
      ticker.innerHTML = "";

      const rows = text.split(/\r?\n/).filter(r => r.trim() !== "");
      const headers = rows.shift().split(",").map(h => h.trim());

      const homeTeamIdx = headers.indexOf("home_team");
      const homeScoreIdx = headers.indexOf("home_score");
      const awayTeamIdx = headers.indexOf("away_team");
      const awayScoreIdx = headers.indexOf("away_score");
      const homeRecordIdx = headers.indexOf("home_record");
      const awayRecordIdx = headers.indexOf("away_record");
      const isTestIdx = headers.indexOf("is_test");
      const datetimeIdx = headers.indexOf("game_datetime"); 

      if (homeTeamIdx === -1 || datetimeIdx === -1) {
        container.textContent = "Header mismatch.";
        return;
      }

      // ===== GROUP GAMES BY DATE =====
      const gamesByDate = {};
      const todayStr = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

      rows.forEach(row => {
        const values = row.split(",").map(v => v.trim());
        const isTest = values[isTestIdx] === "true";
        const gameDateTimeStr = values[datetimeIdx];
        if (isTest || !gameDateTimeStr) return;

        // Convert to YYYY-MM-DD
        const [month, day, year] = gameDateTimeStr.split(" ")[0].split("/");
        const gameDateStr = `${year}-${month.padStart(2,"0")}-${day.padStart(2,"0")}`;

        if (!gamesByDate[gameDateStr]) gamesByDate[gameDateStr] = [];
        gamesByDate[gameDateStr].push(values);
      });

      // Sort dates ascending
      const sortedDates = Object.keys(gamesByDate).sort((a, b) => new Date(a) - new Date(b));

      // ===== RENDER SCORES BY DATE =====
      sortedDates.forEach(dateKey => {
        const dateHeader = document.createElement("h2");
        const dateObj = new Date(dateKey);
        dateHeader.textContent = dateObj.toDateString();
        container.appendChild(dateHeader);

        gamesByDate[dateKey].forEach(values => {
          const homeTeam = values[homeTeamIdx];
          const awayTeam = values[awayTeamIdx];
          const homeScore = parseInt(values[homeScoreIdx]);
          const awayScore = parseInt(values[awayScoreIdx]);
          const homeRecord = values[homeRecordIdx] || "";
          const awayRecord = values[awayRecordIdx] || "";

          let homeClass = "tie", awayClass = "tie";
          if (homeScore > awayScore) { homeClass = "winner"; awayClass = "loser"; }
          else if (awayScore > homeScore) { homeClass = "loser"; awayClass = "winner"; }

          const gameRow = document.createElement("div");
          gameRow.className = "game-row";

          gameRow.innerHTML = `
<div class="team left-team">
  <div class="team-name ${homeClass}">${homeTeam}</div>
  <div class="team-record">${homeRecord}</div>
</div>

<div class="score-center">
  <div class="team-score ${homeClass}">${homeScore}</div>
  <div class="center-info">Final</div>
  <div class="team-score ${awayClass}">${awayScore}</div>
</div>

<div class="team right-team">
  <div class="team-name ${awayClass}">${awayTeam}</div>
  <div class="team-record">${awayRecord}</div>
</div>
`;

          container.appendChild(gameRow);

          // ===== ADD TO TODAY'S TICKER =====
          if (dateKey === todayStr) {
            const tickerItem = document.createElement("div");
            tickerItem.className = "ticker-item";
            tickerItem.textContent = `${homeTeam} ${homeScore} - ${awayScore} ${awayTeam} (Final)`;
            ticker.appendChild(tickerItem);
          }
        });
      });
    })
    .catch(err => console.error(err));

});