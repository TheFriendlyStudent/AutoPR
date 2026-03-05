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

  // ===== SCORES =====
  const scoresContainer = document.getElementById("scores");
  const ticker = document.getElementById("scoreTicker");
  let allScoreRows = [];
  let headers, homeTeamIdx, awayTeamIdx, homeScoreIdx, awayScoreIdx,
      homeRecordIdx, awayRecordIdx, isTestIdx, datetimeIdx;

  // Date picker
  const scoresControls = document.createElement("div");
  scoresControls.id = "scoresControls";
  scoresControls.style.marginBottom = "15px";
  scoresControls.innerHTML = `
    <label for="gameDate">Select date: </label>
    <input type="date" id="gameDate">
  `;
  scoresContainer.appendChild(scoresControls);
  const dateInput = document.getElementById("gameDate");

  // ===== Fetch CSV =====
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

      allScoreRows = rows;

      // Default to today
      const today = new Date();
      dateInput.valueAsDate = today;
      renderScores(today);
      renderTickerToday();
    })
    .catch(err => console.error(err));

  function parseCSVDate(str){
    const [month, day, year] = str.split(" ")[0].split("/");
    return new Date(`${year}-${month.padStart(2,"0")}-${day.padStart(2,"0")}`);
  }

  function formatDate(date){
    return date.toISOString().slice(0,10);
  }

  function renderScores(selectedDate){
    const dateStr = formatDate(selectedDate);
    const existingRows = scoresContainer.querySelectorAll(".game-row, .game-date, .no-games-msg");
    existingRows.forEach(el => el.remove());

    const gamesForDate = allScoreRows
      .map(row => row.split(",").map(v => v.trim()))
      .filter(values => values[isTestIdx] !== "true" && values[datetimeIdx])
      .filter(values => formatDate(parseCSVDate(values[datetimeIdx])) === dateStr);

    if(gamesForDate.length === 0){
      const msg = document.createElement("div");
      msg.className = "no-games-msg";
      msg.textContent = "No games for this day.";
      scoresContainer.appendChild(msg);
      return;
    }

    const dateHeader = document.createElement("div");
    dateHeader.className = "game-date";
    dateHeader.textContent = selectedDate.toDateString();
    scoresContainer.appendChild(dateHeader);

    gamesForDate.forEach(values => {
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
    });
  }

  function renderTickerToday(){
    ticker.innerHTML = "";
    const todayStr = formatDate(new Date());
    allScoreRows
      .map(r => r.split(",").map(v => v.trim()))
      .filter(values => values[isTestIdx] !== "true" && values[datetimeIdx])
      .filter(values => formatDate(parseCSVDate(values[datetimeIdx])) === todayStr)
      .forEach(values=>{
        const homeTeam = values[homeTeamIdx];
        const awayTeam = values[awayTeamIdx];
        const homeScore = parseInt(values[homeScoreIdx]);
        const awayScore = parseInt(values[awayScoreIdx]);
        const item = document.createElement("div");
        item.className = "ticker-item";
        item.textContent = `${homeTeam} ${homeScore} - ${awayScore} ${awayTeam} (Final)`;
        ticker.appendChild(item);
      });
  }

  dateInput.addEventListener("change", e=>{
    renderScores(new Date(e.target.value));
  });

  // ===== LIVESTREAMS =====
  const channels = [
    { name:"Staples Boys Basketball", url:"https://www.youtube.com/@staplesboysbasketball", channelId:"UCxxxx" },
    { name:"The Day CT", url:"https://www.youtube.com/@thedayct", channelId:"UCyyyy" },
    { name:"TB860LIVE", url:"https://www.youtube.com/@TB860LIVE", channelId:"UCzzzz" },
    { name:"WHCI", url:"https://www.youtube.com/@whci", channelId:"UCaaaa" },
    { name:"Newington High School", url:"https://www.youtube.com/@NewingtonHighSchool605", channelId:"UCbbbb" },
    { name:"Project Purple Sports", url:"https://www.youtube.com/@ProjectPurpleSports", channelId:"UCcccc" },
    { name:"Waterbury Public Schools", url:"https://www.youtube.com/@waterburypublicschoolsathl9870", channelId:"UCdddd" }
  ];

  async function fetchChannelInfo(channel){
    const apiKey = "AIzaSyD3gdXcfW4o-JJaDPbgshV573llKx1NOLQ";
    try{
      const liveRes = await fetch(`https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${channel.channelId}&eventType=live&type=video&key=${apiKey}`);
      const liveData = await liveRes.json();

      const logoRes = await fetch(`https://www.googleapis.com/youtube/v3/channels?part=snippet&id=${channel.channelId}&key=${apiKey}`);
      const logoData = await logoRes.json();
      const logo = logoData.items[0].snippet.thumbnails.default.url;

      if(liveData.items && liveData.items.length>0){
        return {...channel, live:true, title:liveData.items[0].snippet.title, url:`https://www.youtube.com/watch?v=${liveData.items[0].id.videoId}`, logo};
      }
      return {...channel, live:false, logo};
    }catch(e){
      console.error(e);
      return {...channel, live:false, logo:""};
    }
  }

  async function renderLivestreams(){
    const listContainer = document.querySelector(".channel-list");
    listContainer.innerHTML = "";

    const results = await Promise.all(channels.map(fetchChannelInfo));
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

  renderLivestreams();

  // ===== AUTO REFRESH =====
  setInterval(()=>{
    renderScores(new Date(dateInput.value));
    renderTickerToday();
    renderLivestreams();
  }, 2*60*1000);

});