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

  // ===== SCORES SECTION =====
  const scoresContainer = document.getElementById("scores");
  const ticker = document.getElementById("scoreTicker");
  let allScoreRows = []; // store CSV rows
  let headers, homeTeamIdx, awayTeamIdx, homeScoreIdx, awayScoreIdx,
      homeRecordIdx, awayRecordIdx, isTestIdx, datetimeIdx;

  // Add date picker
  const scoresControls = document.createElement("div");
  scoresControls.id = "scoresControls";
  scoresControls.style.marginBottom = "15px";
  scoresControls.innerHTML = `
    <label for="gameDate">Select date: </label>
    <input type="date" id="gameDate">
  `;
  scoresContainer.appendChild(scoresControls);
  const dateInput = document.getElementById("gameDate");

  // fetch CSV
  fetch("games.csv")
    .then(res => res.text())
    .then(text => {
      const rows = text.split(/\r?\n/).filter(r => r.trim() !== "");
      headers = rows.shift().split(",").map(h => h.trim());
      homeTeamIdx = headers.indexOf("home_team");
      awayTeamIdx = headers.indexOf("away_team");
      homeScoreIdx = headers.indexOf("home_score");
      awayScoreIdx = headers.indexOf("away_score");
      homeRecordIdx = headers.indexOf("home_record");
      awayRecordIdx = headers.indexOf("away_record");
      isTestIdx = headers.indexOf("is_test");
      datetimeIdx = headers.indexOf("game_datetime");

      if(homeTeamIdx === -1 || datetimeIdx === -1){
        scoresContainer.textContent = "Header mismatch.";
        return;
      }

      allScoreRows = rows; // save rows

      // default to today
      const today = new Date();
      dateInput.valueAsDate = today;
      renderScoresForDate(today);
    })
    .catch(err => console.error(err));

  function renderScoresForDate(selectedDate){
    // clear previous
    scoresContainer.querySelectorAll(".game-row, .game-date, .no-games-msg").forEach(el => el.remove());
    ticker.innerHTML = "";

    const dateStr = selectedDate.toISOString().slice(0,10); // YYYY-MM-DD
    let hasGames = false;

    allScoreRows.forEach(row => {
      const values = row.split(",").map(v => v.trim());
      const isTest = values[isTestIdx] === "true";
      const gameDateTimeStr = values[datetimeIdx];
      if(isTest || !gameDateTimeStr) return;

      // parse CSV date
      const [month, day, year] = gameDateTimeStr.split(" ")[0].split("/");
      const gameDateStr = `${year}-${month.padStart(2,"0")}-${day.padStart(2,"0")}`;
      if(gameDateStr !== dateStr) return;

      hasGames = true;

      const homeTeam = values[homeTeamIdx];
      const awayTeam = values[awayTeamIdx];
      const homeScore = parseInt(values[homeScoreIdx]);
      const awayScore = parseInt(values[awayScoreIdx]);
      const homeRecord = values[homeRecordIdx] || "";
      const awayRecord = values[awayRecordIdx] || "";

      let homeClass = "tie", awayClass = "tie";
      if(homeScore > awayScore){ homeClass="winner"; awayClass="loser"; }
      else if(awayScore > homeScore){ homeClass="loser"; awayClass="winner"; }

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
      scoresContainer.appendChild(gameRow);

      // add to ticker
      const tickerItem = document.createElement("div");
      tickerItem.className = "ticker-item";
      tickerItem.textContent = `${homeTeam} ${homeScore} - ${awayScore} ${awayTeam} (Final)`;
      ticker.appendChild(tickerItem);
    });

    if(!hasGames){
      const msg = document.createElement("div");
      msg.className = "no-games-msg";
      msg.textContent = "No games for this day.";
      scoresContainer.appendChild(msg);
    }
  }

  dateInput.addEventListener("change", e => {
    renderScoresForDate(new Date(e.target.value));
  });

  // refresh scores every 2 minutes
  setInterval(()=>{
    renderScoresForDate(new Date(dateInput.value));
  }, 2*60*1000);


  // ===== LIVESTREAM SECTION =====
  const channels = [
    { name:"Staples Boys Basketball", url:"https://www.youtube.com/@staplesboysbasketball", channelId:"UCxxxx", logo:"logos/staples.png" },
    { name:"The Day CT", url:"https://www.youtube.com/@thedayct", channelId:"UCyyyy", logo:"logos/dayct.png" },
    { name:"TB860LIVE", url:"https://www.youtube.com/@TB860LIVE", channelId:"UCzzzz", logo:"logos/tb860.png" },
    { name:"WHCI", url:"https://www.youtube.com/@whci", channelId:"UCaaaa", logo:"logos/whci.png" },
    { name:"Newington High School", url:"https://www.youtube.com/@NewingtonHighSchool605", channelId:"UCbbbb", logo:"logos/newington.png" },
    { name:"Project Purple Sports", url:"https://www.youtube.com/@ProjectPurpleSports", channelId:"UCcccc", logo:"logos/projectpurple.png" },
    { name:"Waterbury Public Schools", url:"https://www.youtube.com/@waterburypublicschoolsathl9870", channelId:"UCdddd", logo:"logos/waterbury.png" }
  ];

  async function checkLive(channel){
    try{
      const apiKey = "YOUR_YOUTUBE_API_KEY"; // replace
      const res = await fetch(`https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${channel.channelId}&eventType=live&type=video&key=${apiKey}`);
      const data = await res.json();
      if(data.items && data.items.length > 0){
        return { live:true, title: data.items[0].snippet.title, url:`https://www.youtube.com/watch?v=${data.items[0].id.videoId}` };
      }
      return { live:false };
    }catch(e){
      console.error(e);
      return { live:false };
    }
  }

  async function renderChannels(){
    const listContainer = document.querySelector(".channel-list");
    listContainer.innerHTML = "";

    const results = await Promise.all(channels.map(async c=>{
      const liveStatus = await checkLive(c);
      return {...c, ...liveStatus};
    }));

    results.sort((a,b)=>{
      if(a.live && !b.live) return -1;
      if(!a.live && b.live) return 1;
      return a.name.localeCompare(b.name);
    });

    results.forEach(channel=>{
      const row = document.createElement("div");
      row.className = "channel-row";
      row.innerHTML = `
        <img class="channel-logo" src="${channel.logo}" alt="${channel.name}">
        <div class="channel-info">
          <div class="channel-name">${channel.name}</div>
          ${channel.live ? `<div class="live-indicator">LIVE: ${channel.title}</div>` : `<div>Offline</div>`}
        </div>
        <a class="watch-button" href="${channel.url}" target="_blank">Watch</a>
      `;
      listContainer.appendChild(row);
    });
  }

  // initial render
  renderChannels();
  // refresh every 2 minutes
  setInterval(renderChannels, 2*60*1000);

});