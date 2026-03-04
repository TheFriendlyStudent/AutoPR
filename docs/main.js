fetch("games.csv")
  .then(response => response.text())
  .then(text => {
    const container = document.getElementById("scores");
    
    // Split CSV into rows
    const rows = text.split("\n").filter(row => row.trim() !== "");
    
    // Extract header row
    const headers = rows[0].split(",").map(h => h.trim());
    
    // Find the correct column indexes by name
    const homeTeamIdx = headers.indexOf("home_team");
    const awayTeamIdx = headers.indexOf("away_team");
    const homeScoreIdx = headers.indexOf("home_score");
    const awayScoreIdx = headers.indexOf("away_score");
    
    // Loop through each row except the header
    for (let i = 1; i < rows.length; i++) {
      const cols = rows[i].split(",").map(c => c.trim());
      
      // Skip rows that are incomplete
      if (!cols[homeTeamIdx] || !cols[awayTeamIdx] || !cols[homeScoreIdx] || !cols[awayScoreIdx]) continue;
      
      const div = document.createElement("div");
      div.textContent = `${cols[homeTeamIdx]} ${cols[homeScoreIdx]} - ${cols[awayScoreIdx]} ${cols[awayTeamIdx]}`;
      container.appendChild(div);
    }
  })
  .catch(err => console.error("Error loading CSV:", err));