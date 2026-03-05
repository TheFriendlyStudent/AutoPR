// ===== main.js =====
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

  // ===== UPDATE SCORES =====
  async function updateScores() {
    const container = document.getElementById("scores");
    const ticker = document.getElementById("scoreTicker");
    container.innerHTML = "";
    ticker.innerHTML = "";

    try {
      const res = await fetch("games.csv");
      const text = await res.text();

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

      const today = new Date();
      const todayStr = today.toISOString().slice(0, 10);

      rows.forEach(row => {
        const values = row.split(",").map(v => v.trim());
        const homeTeam = values[homeTeamIdx];
        const awayTeam = values[awayTeamIdx];
        const homeScore = parseInt(values[homeScoreIdx]);
        const awayScore = parseInt(values[awayScoreIdx]);
        const homeRecord = values[homeRecordIdx] || "";
        const awayRecord = values[awayRecordIdx] || "";
        const isTest = values[isTestIdx] === "true";
        const gameDateTimeStr = values[datetimeIdx];

        if (isTest || !gameDateTimeStr) return;

        const [month, day, year] = gameDateTimeStr.split(" ")[0].split("/");
        const gameDateStr = `${year}-${month.padStart(2,"0")}-${day.padStart(2,"0")}`;
        if (gameDateStr !== todayStr) return;

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

        // Add to ticker
        const tickerItem = document.createElement("div");
        tickerItem.className = "ticker-item";
        tickerItem.textContent = `${homeTeam} ${homeScore} - ${awayScore} ${awayTeam} (Final)`;
        ticker.appendChild(tickerItem);
      });
    } catch (err) {
      console.error("Error updating scores:", err);
    }
  }

  // ===== LIVESTREAMS =====
  const API_KEY = "YOUR_YOUTUBE_API_KEY"; // <-- replace with your API key

  async function checkLive(channel) {
    try {
      const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${channel.channelId}&type=video&eventType=live&key=${API_KEY}`;
      const res = await fetch(url);
      const data = await res.json();

      if (data.items && data.items.length > 0) {
        const liveVideo = data.items[0];
        channel.live = true;
        channel.videoId = liveVideo.id.videoId;
        channel.streamTitle = liveVideo.snippet.title;
      } else {
        channel.live = false;
        channel.videoId = null;
        channel.streamTitle = null;
      }
      return channel;
    } catch (err) {
      console.error(`Error checking live for ${channel.name}:`, err);
      channel.live = false;
      return channel;
    }
  }

  function renderChannels(channels) {
    const container = document.getElementById("livestreams");
    container.innerHTML = "";

    const list = document.createElement("div");
    list.className = "channel-list";

    channels.forEach(channel => {
      const item = document.createElement("div");
      item.className = "channel-item";

      item.innerHTML = `
        <img src="${channel.logo}" alt="${channel.name}" class="channel-logo" />
        <div class="channel-info">
          <div class="channel-name">${channel.name}</div>
          ${channel.live ? `<div class="stream-title">LIVE: ${channel.streamTitle}</div>` : `<div class="offline">Offline</div>`}
          ${channel.live ? `<a href="https://www.youtube.com/watch?v=${channel.videoId}" target="_blank" class="watch-btn">Watch</a>` : ""}
        </div>
      `;
      list.appendChild(item);
    });

    container.appendChild(list);
  }

  async function updateLivestreams() {
    try {
      const res = await fetch("channels.json");
      const channels = await res.json();

      const checkedChannels = await Promise.all(channels.map(checkLive));

      // Sort live first, then alphabetical
      checkedChannels.sort((a, b) => {
        if (a.live && !b.live) return -1;
        if (!a.live && b.live) return 1;
        return a.name.localeCompare(b.name);
      });

      renderChannels(checkedChannels);
    } catch (err) {
      console.error("Error updating livestreams:", err);
    }
  }

  // ===== INITIAL LOAD & AUTO REFRESH =====
  updateScores();
  updateLivestreams();

  // Refresh every 2 minutes
  setInterval(updateScores, 120000);
  setInterval(updateLivestreams, 120000);
});