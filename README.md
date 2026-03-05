# Nutmeg Sports

## CSV Structure

### `docs/master_games.csv` — Source of Truth
Admin-approved scheduled games and final scores. All other parts of the system read from this file.

| Column | Description |
|---|---|
| `game_id` | Unique ID (e.g. `game_001`) |
| `header` | Display header (e.g. "CIAC Boys Basketball") |
| `home_team` / `away_team` | Team names |
| `home_rank` / `away_rank` | Rankings (NR if unranked) |
| `home_score` / `away_score` | Final scores (empty if not yet played) |
| `home_record` / `away_record` | Season records |
| `bg_image` | Google Drive link to background photo |
| `photo_cred` | Photo credit handle |
| `game_datetime` | `MM/DD/YYYY HH:MM:SS` format |
| `status` | `scheduled` or `final` |
| `posted_to_instagram` | `true` / `false` — set by `test.py` after posting |
| `caption` | Short label shown on graphic (e.g. FINAL, BELT, UPSET) |

**Google Sheets:** Keep a `master_games` worksheet tab in your linked spreadsheet. `getScoreSheet.py` syncs it to this CSV every 20 minutes via GitHub Actions.

---

### `docs/submitted_scores.csv` — User Submissions
Score reports submitted through the website form. These are **not** automatically published — they must be reviewed and approved in the Admin Console first.

| Column | Description |
|---|---|
| `submission_id` | Auto-generated ID |
| `game_id` | References a game in `master_games.csv` |
| `home_score` / `away_score` | Submitted scores (validated as integers 0–999) |
| `image_url` | URL of the uploaded game photo |
| `submitter_note` | Optional note from submitter |
| `submitted_at` | ISO timestamp |
| `status` | `pending`, `approved`, or `rejected` |
| `admin_notes` | Admin review notes |

---

### `docs/predictions.csv` — Vote Records
One row per vote cast on the website. Voting opens 48 hours before a game and closes at tip-off.

| Column | Description |
|---|---|
| `vote_id` | Auto-generated |
| `game_id` | Game being predicted |
| `predicted_winner` | `home` or `away` |
| `voted_at` | ISO timestamp |
| `voter_fingerprint` | Anonymous session ID |

---

## Website Features

### Scores Tab
- Shows games for any selected date
- Displays final scores with win/loss styling and caption badges
- Upcoming games show tip-off time

### Prediction Voting
- Appears on game cards 48 hours before tip-off
- Closes automatically at game start
- Results shown as a visual bar with percentages

### Submit Score Form
- Users select a game from approved upcoming/scheduled games
- Enter home and away scores (validated: integers, 0–999)
- Upload a game photo
- Submissions go to `submitted_scores.csv` with `status: pending`

**Master Schedule tab:**
- Edit any field on any game inline
- Add new games to the schedule
- Delete games
- Export updated `master_games.csv` to replace in repo

**Submissions tab:**
- View all pending submissions with photo previews
- **Approve:** automatically applies the score to the master game and marks it `final`
- **Reject:** marks submission as rejected
- Add admin notes
- Export `submitted_scores.csv`

---

## Instagram Posting Flow

1. GitHub Actions syncs `master_games.csv` from Google Sheets every 20 min
2. Run `test.py` manually (or via `upload_graphics.yml` workflow) to post
3. `test.py` calls `render_from_csv()` which skips games where `posted_to_instagram=true` or `status != final`
4. After successful Instagram post, `mark_posted()` sets `posted_to_instagram=true` for each game
5. Updated CSV is committed back to the repo

---

## Setup Notes

- Add a `master_games` worksheet tab to your Google Sheet (in addition to existing tabs)
- Add `submitted_scores` and `predictions` worksheet tabs (can start empty with just headers)
- Change `ADMIN_PIN` in `docs/main.js` before deploying
- The `main.js` file has been removed — all JS is now inline in `index.html`
