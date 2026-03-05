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
      const awayTeamIdx = headers.indexOf("away_team");
      const homeScoreIdx = headers.indexOf("home_score");
      const awayScoreIdx = headers.indexOf("away_score");
      const homeRecordIdx = headers.indexOf("home_record");
      const awayRecordIdx = headers.indexOf("away_record");
      const isTestIdx = headers.indexOf("is_test");
      const datetimeIdx = headers.indexOf("game_datetime");

      if (homeTeamIdx === -1 || datetimeIdx === -1) {
        container.textContent = "Header mismatch.";
        return;
      }

      const today = new Date();
      const todayStr = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;

      let hasGames = false;

      rows.forEach(row => {
        const values = row.split(",").map(v => v.trim());
        const homeTeam = values[homeTeamIdx];
        const awayTeam = values[awayTeamIdx];
        const homeScore = parseInt(values[homeScoreIdx]);
        const awayScore = parseInt(values[awayScoreIdx]);
        const homeRecord = values[homeRecordIdx] || "";
        const awayRecord = values[awayRecordIdx] || "";
        const isTest = values[isTestIdx] === "true";
        const gameDateTimeStr = values[datetimeIdx]; // e.g. "03/04/2026 19:00:00"

        if (isTest || !gameDateTimeStr) return;

        // Convert CSV datetime to YYYY-MM-DD
        const [month, day, year] = gameDateTimeStr.split(" ")[0].split("/");
        const gameDateStr = `${year}-${month.padStart(2,"0")}-${day.padStart(2,"0")}`;

        if (gameDateStr !== todayStr) return; // skip games not today
        hasGames = true;

        let homeClass = "tie", awayClass = "tie";
        if (homeScore > awayScore) { homeClass = "winner"; awayClass = "loser"; }
        else if (awayScore > homeScore) { homeClass = "loser"; awayClass = "winner"; }

        // Add date header once
        if (!container.querySelector(".game-date")) {
          const dateHeader = document.createElement("div");
          dateHeader.className = "game-date";
          dateHeader.textContent = `Games for ${todayStr}`;
          container.appendChild(dateHeader);
        }

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

        // Add to ticker
        const tickerItem = document.createElement("div");
        tickerItem.className = "ticker-item";
        tickerItem.textContent = `${homeTeam} ${homeScore} - ${awayScore} ${awayTeam} (Final)`;
        ticker.appendChild(tickerItem);
      });

      if (!hasGames) {
        container.textContent = "No games today.";
      }
    })
    .catch(err => console.error(err));

  // ===== LIVESTREAM CHANNELS =====
  const channels = [
    { name: "Staples Boys Basketball", url: "https://www.youtube.com/@staplesboysbasketball" },
    { name: "The Day CT", url: "https://www.youtube.com/@thedayct" },
    { name: "TB860LIVE", url: "https://www.youtube.com/@TB860LIVE" },
    { name: "WHCI", url: "https://www.youtube.com/@whci" },
    { name: "Newington High School", url: "https://www.youtube.com/@NewingtonHighSchool605" },
    { name: "Project Purple Sports", url: "https://www.youtube.com/@ProjectPurpleSports" },
    { name: "Waterbury Public Schools", url: "https://www.youtube.com/@waterburypublicschoolsathl9870" }
  ];

  const listContainer = document.querySelector(".channel-list");

  channels.forEach(channel => {
    const row = document.createElement("div");
    row.className = "channel-row";
    row.innerHTML = `
      <div class="channel-info">
        <div class="channel-name">${channel.name}</div>
        <a class="watch-button" href="${channel.url}" target="_blank">Watch</a>
      </div>
    `;
    listContainer.appendChild(row);
  });

});